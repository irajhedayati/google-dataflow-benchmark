package ca.dataedu.dataflow.benchmark

import com.google.cloud.monitoring.v3.MetricServiceClient
import com.google.monitoring.v3.Aggregation.{Aligner, Reducer}
import com.google.monitoring.v3.ListTimeSeriesRequest.TimeSeriesView
import com.google.monitoring.v3.{Aggregation, ListTimeSeriesRequest, ProjectName}
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser._

import java.io.PrintWriter
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters._

class Analyze(config: Config, metricServiceClient: MetricServiceClient) extends LazyLogging {

  case class DataflowMetric(name: Metric, scalar: Long)

  case class Metric(name: String)

  implicit val dataflowMetricDecoder: Decoder[DataflowMetric] = deriveDecoder[DataflowMetric]
  implicit val metricDecoder: Decoder[Metric] = deriveDecoder[Metric]

  def analyze(
      benchmark: BenchmarkConfig,
      jobId: String,
      start: ZonedDateTime,
      end: ZonedDateTime,
      outputWriter: PrintWriter
  ): Unit = {
    val runTime = Duration.between(start, end)
    val (maxCpu, minCpu, avgCpu) = getCpuUtilization(jobId, start, end)
    val pTransformMaxThroughput = config.outputTransforms.map(getMaxThroughput(jobId, start, end))
    val output = GCloud.runGcloud(config, s"alpha dataflow metrics list $jobId").stdout
    val dataflowMetrics = parse(output)
      .getOrElse(Json.Null)
      .as[List[DataflowMetric]]
      .map(metrics => metrics.map(metric => metric.name.name -> metric.scalar).toMap) match {
      case Left(error) =>
        logger.warn(s"Unable to parse the metrics: $error")
        Map.empty[String, Long]
      case Right(metrics) => metrics
    }

    val vCpuPerHour: Double = dataflowMetrics.get(Metrics.TotalVcpuTime).map(_ / 3600.0).getOrElse(0.0)
    val memGbPerHour: Double = dataflowMetrics.get(Metrics.TotalMemoryUsage).map(_ / 1024.0 / 3600.0).getOrElse(0.0)
    val hddGbPerHour: Double = dataflowMetrics.get(Metrics.TotalPdUsage).map(_ / 3600.0).getOrElse(0.0)
    val ssdGbPerHour: Double = dataflowMetrics.get(Metrics.TotalSsdUsage).map(_ / 3600.0).getOrElse(0.0)
    /*
    Dataflow resources cost factors (showing us-central-1 pricing).
    See https://cloud.google.com/dataflow/pricing#pricing-details
    The idea is to provide an estimate to compare.
     */
    val VCpuPerHrStreaming: Double = 0.069
    val MemPerGbHrStreaming: Double = 0.0035557
    val PdPerGbHr: Double = 0.000054
    val SddPerGbHr: Double = 0.000298
    val cost = (vCpuPerHour * VCpuPerHrStreaming) + (memGbPerHour * MemPerGbHrStreaming) +
      (hddGbPerHour * PdPerGbHr) + (ssdGbPerHour * SddPerGbHr)

    outputWriter.write(s"${benchmark.name},$jobId")
    outputWriter.write(s",${runTime.toSeconds},${maxCpu * 100},${minCpu * 100},${avgCpu * 100}")
    outputWriter.write(s",${config.numberOfRecords / runTime.toSeconds},$cost")

    outputWriter.write("," + dataflowMetrics.get(Metrics.TotalVcpuTime).map(_.toString).getOrElse("N/A"))
    outputWriter.write("," + dataflowMetrics.get(Metrics.TotalMemoryUsage).map(_.toString).getOrElse("N/A"))
    outputWriter.write("," + dataflowMetrics.get(Metrics.TotalPdUsage).map(_.toString).getOrElse("N/A"))
    outputWriter.write("," + dataflowMetrics.get(Metrics.TotalSsdUsage).map(_.toString).getOrElse("N/A"))
    outputWriter.write("," + dataflowMetrics.get(Metrics.TotalShuffleDataProcessed).map(_.toString).getOrElse("N/A"))
    outputWriter.write("," + dataflowMetrics.get(Metrics.BillableShuffleDataProcessed).map(_.toString).getOrElse("N/A"))
    if (pTransformMaxThroughput.nonEmpty)
      outputWriter.write(s",${pTransformMaxThroughput.mkString(",")}")
    outputWriter.write("\n")
    logger.info(s"Finished analyzing the metrics of job $jobId")
  }

  /** CPU metrics data can take up to 240 seconds to appear */
  private def getCpuUtilization(
      jobId: String,
      start: ZonedDateTime,
      end: ZonedDateTime
  ): (Double, Double, Double) = {
    val now = Time.nowInUtc
    val nowMinusEnd = Duration.between(end, now)
    if (nowMinusEnd.compareTo(Time.DataflowMetricDelay) < 0) {
      logger.info("Waiting for CPU metrics to be available (up to 4 minutes)...")
      Thread.sleep(Time.DataflowMetricDelay.minus(nowMinusEnd).toMillis)
    }
    val request = ListTimeSeriesRequest
      .newBuilder()
      .setName(ProjectName.of(config.project).toString)
      .setFilter(s"""metric.type = "compute.googleapis.com/instance/cpu/utilization" AND
           |resource.labels.project_id = ${config.project} AND
           |metadata.user_labels.dataflow_job_id = "$jobId"""".stripMargin)
      .setView(TimeSeriesView.FULL)
      .setInterval(Time.timeInterval(start, end))
      .setAggregation(
        Aggregation
          .newBuilder()
          .setAlignmentPeriod(com.google.protobuf.Duration.newBuilder().setSeconds(60).build())
          .setPerSeriesAligner(Aligner.ALIGN_MEAN)
          .setCrossSeriesReducer(Reducer.REDUCE_MEAN)
          .addGroupByFields("resource.instance_id")
          .build()
      )
      .build()

    val points = metricServiceClient
      .listTimeSeries(request)
      .getPage
      .getResponse
      .getTimeSeriesList
      .asScala
      .flatMap(_.getPointsList.asScala)
      .map(_.getValue.getDoubleValue)
    val avg = points.sum / points.size
    (points.max, points.min, scala.math.round(avg * 100.0) / 100.0)
  }

  private def getMaxThroughput(jobId: String, start: ZonedDateTime, end: ZonedDateTime)(pTransform: String): Double = {
    val now = Time.nowInUtc
    val nowMinusEnd = Duration.between(end, now)
    if (nowMinusEnd.compareTo(Time.DataflowMetricDelay) < 0) {
      logger.info("Waiting for metrics to be available (up to 4 minutes)...")
      Thread.sleep(Time.DataflowMetricDelay.minus(nowMinusEnd).toMillis)
    }
    val request = ListTimeSeriesRequest
      .newBuilder()
      .setName(ProjectName.of(config.project).toString)
      .setFilter(s"""metric.type = "dataflow.googleapis.com/job/elements_produced_count" AND
           |resource.labels.project_id = ${config.project} AND
           |metric.labels.job_id = "$jobId" AND
           |metric.labels.ptransform = "$pTransform"""".stripMargin)
      .setView(TimeSeriesView.FULL)
      .setInterval(Time.timeInterval(start, end))
      .setAggregation(
        Aggregation
          .newBuilder()
          .setAlignmentPeriod(com.google.protobuf.Duration.newBuilder().setSeconds(60).build())
          .setPerSeriesAligner(Aligner.ALIGN_MEAN)
          .build()
      )
      .build()
    val values = metricServiceClient
      .listTimeSeries(request)
      .getPage
      .getResponse
      .getTimeSeriesList
      .asScala
      .flatMap(_.getPointsList.asScala)
      .map(_.getValue.getDoubleValue)
    if (values.nonEmpty) values.max
    else -1.0
  }

}
