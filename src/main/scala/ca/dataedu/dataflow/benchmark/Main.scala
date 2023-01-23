package ca.dataedu.dataflow.benchmark

import com.google.cloud.monitoring.v3.MetricServiceClient
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, SubscriptionAdminSettings}
import com.google.monitoring.v3.Aggregation.{Aligner, Reducer}
import com.google.monitoring.v3.ListTimeSeriesRequest.TimeSeriesView
import com.google.monitoring.v3.{Aggregation, ListTimeSeriesRequest, ProjectName, TimeInterval}
import com.google.protobuf.Timestamp
import com.google.pubsub.v1.{CreateSnapshotRequest, SeekRequest, SnapshotName}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.parser._

import java.io.{File, PrintWriter}
import java.time.{Duration, ZoneId, ZonedDateTime}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Random, Success, Try}

object Main extends LazyLogging {

  private val jobIdRe = "Submitted job: (.\\S*)".r
  private val subAdmin: SubscriptionAdminClient = SubscriptionAdminClient.create(
    SubscriptionAdminSettings
      .create(SubscriberStubSettings.newBuilder().build())
  )
  private val metricServiceClient = MetricServiceClient.create()
  private val DataflowMetricDelay = Duration.ofMinutes(4)
  private def nowInUtc: ZonedDateTime = ZonedDateTime.now(ZoneId.of("UTC"))
   def timeInterval(start: ZonedDateTime, end: ZonedDateTime): TimeInterval = TimeInterval
    .newBuilder()
    .setStartTime(Timestamp.newBuilder().setSeconds(start.toEpochSecond).build())
    .setEndTime(Timestamp.newBuilder().setSeconds(end.toEpochSecond).build())
    .build()

  def main(args: Array[String]): Unit = {
    val config = Config("google-dataflow-benchmark")
    // Prepare the output file and write the header
    val outputWriter = new PrintWriter(new File(config.outputFile))
    outputWriter.write("name,job_id,run_time_seconds")
    outputWriter.write(",max_cpu_utilization_percent,min_cpu_utilization_percent,avg_cpu_utilization_percent")
    outputWriter.write(",throughput_rps,estimated_cost_per_hour")
    outputWriter.write("," + Metrics.TotalVcpuTime)
    outputWriter.write("," + Metrics.TotalMemoryUsage)
    outputWriter.write("," + Metrics.TotalPdUsage)
    outputWriter.write("," + Metrics.TotalSsdUsage)
    outputWriter.write("," + Metrics.TotalShuffleDataProcessed)
    outputWriter.write("," + Metrics.BillableShuffleDataProcessed)
    if (config.outputTransforms.nonEmpty)
      outputWriter.write(s",${config.outputTransforms.mkString(",")}")
    outputWriter.write("\n")

    Try(
      // run all the benchmarks
      config.benchmarks.foreach { benchmark =>
        logger.info(s"Starting benchmark ${benchmark.name}")
        val snapShotName = prepare(config)
        run(config, benchmark).map { case (jobId, created, finished) =>
          logger.info("Start analyzing the metrics")
          analyze(config, benchmark, jobId, created, finished, outputWriter)
          jobId
        } match {
          case Success(jobId) => logger.info(s"Successfully finished the job ID $jobId")
          case Failure(error) =>
            logger.info(s"Failed to run the job. Check out Google console for any resources", error)
        }
        cleanup(config, snapShotName)
      }
    )
    outputWriter.flush()
    outputWriter.close()
    subAdmin.shutdown()
    metricServiceClient.shutdown()
  }

  /** Create a snapshot from the subscription */
  private def prepare(config: Config): SnapshotName = {
    val alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
    def randStr(n: Int) = (1 to n).map(_ => alpha(Random.nextInt(alpha.length))).mkString
    val snapshotName =
      SnapshotName.of(config.project, s"${config.inputPubSubSubscription.getSubscription}-${randStr(6)}")
    logger.info(s"creating a snapshot $snapshotName from the input subscription")
    val request = CreateSnapshotRequest
      .newBuilder()
      .setName(snapshotName.toString)
      .setSubscription(config.inputPubSubSubscription.toString)
      .build()

    subAdmin.createSnapshot(request)
    logger.info(s"Created snapshot $snapshotName for input subscription data")
    snapshotName
  }

  private def run(config: Config, benchmarkConfig: BenchmarkConfig): Try[(String, ZonedDateTime, ZonedDateTime)] = {
    val dataflowArgs = List(
      s"--project=${config.project}",
      s"--region=${config.region}",
      s"--numWorkers=${benchmarkConfig.numberOfWorkers}",
      s"--maxNumWorkers=${benchmarkConfig.maxNumberOfWorkers}",
      s"--workerMachineType=${benchmarkConfig.machineType}",
      s"--diskSizeGb=${benchmarkConfig.diskSizeGb}",
      s"--gcpTempLocation=gs://${config.gcpBucket}/temp",
      s"--stagingLocation=gs://${config.gcpBucket}/staging",
      s"--defaultWorkerLogLevel=${config.defaultWorkerLogLevel}"
    ).mkString(" ")

    val command =
      s"""java -cp ${config.jarFile} ${config.className.getOrElse("")}
         | --runner=DataflowRunner $dataflowArgs
         |  ${benchmarkConfig.applicationArgs.mkString(" ")}""".stripMargin
    Try(command !!).flatMap { result =>
      jobIdRe
        .findFirstMatchIn(result)
        .map(_.group(1))
        .map(jobId => waitFormCompletion(config, jobId))
        .map(jobId => drain(config, jobId)) match {
        case Some(Right(jobIdAndDuration)) => Success(jobIdAndDuration)
        case Some(Left(error))             => Failure(error)
        case None => Failure(new RuntimeException("Failed to run the job. Check out Google console for any resources"))
      }
    }
  }

  private case class CommandResult(stdout: String, stderr: String, status: Int)

  /** It will add --project, --region, and --format=json argument
    *
    * @param config
    *   to get the path to `gcloud` binary as well as project and region
    * @param command
    *   the command to run
    * @return
    *   standard output and error as well the status code
    */
  private def runGcloud(config: Config, command: String): CommandResult = {
    val commandToRun =
      s"${config.gcloudPath} $command --project=${config.project} --region=${config.region} --format=json"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val status = commandToRun ! ProcessLogger(stdout append _, stderr append _)
    CommandResult(stdout.toString(), stderr.toString(), status)
  }

  private def drain(config: Config, jobId: String): Either[io.circe.Error, (String, ZonedDateTime, ZonedDateTime)] = {
    logger.info("The subscription is exhausted, killing the job")

    @tailrec
    def tryDrain(): Unit = {
      val result = runGcloud(config, s"dataflow jobs drain $jobId")

      if (result.stderr.nonEmpty && result.stderr.startsWith("Failed")) {
        Thread.sleep(10000)
        tryDrain()
      }
    }
    tryDrain()

    @tailrec
    def waitToDrain(): Either[io.circe.Error, (ZonedDateTime, ZonedDateTime)] = {
      val output = runGcloud(config, s"dataflow jobs describe $jobId").stdout
      parse(output)
        .map(_.hcursor)
        .flatMap { cursor =>
          for {
            isDrained <- cursor.downField("currentState").as[String].map(_ == "JOB_STATE_DRAINED")
            createTime <- cursor.downField("createTime").as[String].map(ZonedDateTime.parse)
            currentStateTime <- cursor.downField("currentStateTime").as[String].map(ZonedDateTime.parse)
          } yield (isDrained, createTime, currentStateTime)
        } match {
        case Left(error) =>
          logger.error(s"Unable to parse the state from 'describe' command: $output", error)
          Left(error)
        case Right((true, createTime, currenStateTime)) =>
          logger.info(s"Job $jobId is drained successfully")
          Right(createTime -> currenStateTime)
        case Right((false, _, _)) =>
          logger.info(s"Job $jobId is not yet drained... wait 10 more seconds and check again")
          Thread.sleep(10000)
          waitToDrain()
      }
    }
    waitToDrain().map(duration => (jobId, duration._1, duration._2))
  }

  private def waitFormCompletion(config: Config, jobId: String): String = {
    logger.info(s"Job $jobId is created, waiting for completion")

    /** Checks the number of undelivered messages from the Pub/Sub subscription. If it is 0, then the job is complete
      * and if not, it will try after 10 seconds.
      */
    @tailrec
    def waitForComplete(config: Config): Unit = {
      logger.info("Check the backlog size")
      val now = nowInUtc
      val request = ListTimeSeriesRequest
        .newBuilder()
        .setName(ProjectName.of(config.project).toString)
        .setFilter(
          s"""metric.type = "pubsub.googleapis.com/subscription/backlog_bytes" AND
             |resource.labels.subscription_id = "${config.inputPubSubSubscription.getSubscription}"""".stripMargin
        )
        .setView(TimeSeriesView.FULL)
        .setInterval(timeInterval(now.minus(Duration.ofMinutes(5)), now))
        .build()

      val page = metricServiceClient.listTimeSeries(request).getPage
      if (page.getValues.asScala.nonEmpty) {
        val backlogSize = page.getResponse
          .getTimeSeries(0)
          .getPoints(0)
          .getValue
          .getInt64Value
        logger.info(
          s"Polling: Backlog size of subscription ${config.inputPubSubSubscription.getSubscription} is $backlogSize"
        )
        if (backlogSize == 0) {
          return
        }
      }
      Thread.sleep(10000)
      waitForComplete(config)
    }

    waitForComplete(config)
    jobId
  }

  import io.circe.Decoder
  import io.circe.generic.semiauto.deriveDecoder

  implicit val dataflowMetricDecoder: Decoder[DataflowMetric] = deriveDecoder[DataflowMetric]
  implicit val metricDecoder: Decoder[Metric] = deriveDecoder[Metric]

  case class DataflowMetric(name: Metric, scalar: Long)
  case class Metric(name: String)
  private def analyze(
      config: Config,
      benchmark: BenchmarkConfig,
      jobId: String,
      start: ZonedDateTime,
      end: ZonedDateTime,
      outputWriter: PrintWriter
  ): Unit = {
    val runTime = Duration.between(start, end)
    val (maxCpu, minCpu, avgCpu) = getCpuUtilization(config, jobId, start, end)
    val pTransformMaxThroughput = config.outputTransforms.map(getMaxThroughput(config, jobId, start, end))
    val output = runGcloud(config, s"alpha dataflow metrics list $jobId").stdout
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
      config: Config,
      jobId: String,
      start: ZonedDateTime,
      end: ZonedDateTime
  ): (Double, Double, Double) = {
    val now = nowInUtc
    val nowMinusEnd = Duration.between(end, now)
    if (nowMinusEnd.compareTo(DataflowMetricDelay) < 0) {
      logger.info("Waiting for CPU metrics to be available (up to 4 minutes)...")
      Thread.sleep(DataflowMetricDelay.minus(nowMinusEnd).toMillis)
    }
    val request = ListTimeSeriesRequest
      .newBuilder()
      .setName(ProjectName.of(config.project).toString)
      .setFilter(s"""metric.type = "compute.googleapis.com/instance/cpu/utilization" AND
                     |resource.labels.project_id = ${config.project} AND
                     |metadata.user_labels.dataflow_job_id = "$jobId"""".stripMargin)
      .setView(TimeSeriesView.FULL)
      .setInterval(timeInterval(start, end))
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

  private def getMaxThroughput(config: Config, jobId: String, start: ZonedDateTime, end: ZonedDateTime)(
      pTransform: String
  ): Double = {
    val now = nowInUtc
    val nowMinusEnd = Duration.between(end, now)
    if (nowMinusEnd.compareTo(DataflowMetricDelay) < 0) {
      logger.info("Waiting for metrics to be available (up to 4 minutes)...")
      Thread.sleep(DataflowMetricDelay.minus(nowMinusEnd).toMillis)
    }
    val request = ListTimeSeriesRequest
      .newBuilder()
      .setName(ProjectName.of(config.project).toString)
      .setFilter(s"""metric.type = "dataflow.googleapis.com/job/elements_produced_count" AND
                    |resource.labels.project_id = ${config.project} AND
                    |metric.labels.job_id = "$jobId" AND
                    |metric.labels.ptransform = "$pTransform"""".stripMargin)
      .setView(TimeSeriesView.FULL)
      .setInterval(timeInterval(start, end))
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

  /** Restore the snapshot and delete it */
  private def cleanup(config: Config, snapshotName: SnapshotName): Unit = {
    logger.info(s"Cleanup: restore the snapshot $snapshotName")
    val request =
      SeekRequest
        .newBuilder()
        .setSubscription(config.inputPubSubSubscription.toString)
        .setSnapshot(snapshotName.toString)
        .build()
    subAdmin.seek(request)
    logger.info(s"Cleanup: The snapshot $snapshotName is restored")
    logger.info(s"Cleanup: delete the snapshot $snapshotName")
    subAdmin.deleteSnapshot(snapshotName)
    logger.info(s"Cleanup: successfully deleted the snapshot $snapshotName")
  }

}
