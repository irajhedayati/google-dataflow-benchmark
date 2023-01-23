package ca.dataedu.dataflow.benchmark

import com.google.cloud.monitoring.v3.MetricServiceClient
import com.google.monitoring.v3.ListTimeSeriesRequest.TimeSeriesView
import com.google.monitoring.v3.{ListTimeSeriesRequest, ProjectName}
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._

import java.time.{Duration, ZonedDateTime}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/** Run the benchmark and wait for the job to finish
  */
class Benchmark(config: Config, metricServiceClient: MetricServiceClient) extends LazyLogging {

  private val jobIdRe = "Submitted job: (.\\S*)".r

  /** Prepares the command to launch the Dataflow job and send the command to Dataflow service. Then waits for the job
    * to complete (the job is complete when the backlog in the input subscription is exhausted) and then drains the job.
    *
    * The execution time here includes warm-up and cool-down period as well. But as all the benchmarks have the same
    * cycle, it can give us a relative run-time to compare.
    *
    * @param benchmarkConfig
    *   the benchmark configuration to run
    * @return
    *   the job ID, the time that job is created, and the time that it's finished
    */
  def run(benchmarkConfig: BenchmarkConfig): Try[(String, ZonedDateTime, ZonedDateTime)] = {
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
        .map(jobId => waitFormCompletion(jobId))
        .map(jobId => drain(jobId)) match {
        case Some(Right(jobIdAndDuration)) => Success(jobIdAndDuration)
        case Some(Left(error))             => Failure(error)
        case None => Failure(new RuntimeException("Failed to run the job. Check out Google console for any resources"))
      }
    }
  }

  /** Checks the backlog size by querying [[Metrics.BacklogBytes]] metric from Metrics service for the input
    * subscription. It will do it repeatedly every 10 seconds.
    */
  private def waitFormCompletion(jobId: String): String = {
    logger.info(s"Job $jobId is created, waiting for completion")

    @tailrec
    def waitForComplete(): Unit = {
      logger.info("Check the backlog size")
      val now = Time.nowInUtc
      val request = ListTimeSeriesRequest
        .newBuilder()
        .setName(ProjectName.of(config.project).toString)
        .setFilter(
          s"""metric.type = "${Metrics.BacklogBytes}" AND
             |resource.labels.subscription_id = "${config.inputPubSubSubscription.getSubscription}"""".stripMargin
        )
        .setView(TimeSeriesView.FULL)
        .setInterval(Time.timeInterval(now.minus(Duration.ofMinutes(5)), now))
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
      waitForComplete()
    }

    waitForComplete()
    jobId
  }

  /** Sends a drain command to Dataflow service for this job. It is possible that the job is not ready to drain, so it
    * will wait 10 seconds and try again. Then waits for the job to drain. It runs a command to describe the job every
    * 10 seconds. Once the job is drained, it will parse the "createTime" and "currentStateTime" (which is the time that
    * the job is fully drained from the output of the command.
    *
    * @param jobId the job ID to drain
    * @return the (job ID, start time, end time) or an error if unable to parse the output of the command.
    */
  private def drain(jobId: String): Either[io.circe.Error, (String, ZonedDateTime, ZonedDateTime)] = {
    logger.info("The subscription is exhausted, killing the job")

    @tailrec
    def tryDrain(): Unit = {
      val result = GCloud.runGcloud(config, s"dataflow jobs drain $jobId")

      if (result.stderr.nonEmpty && result.stderr.startsWith("Failed")) {
        Thread.sleep(10000)
        tryDrain()
      }
    }

    tryDrain()

    @tailrec
    def waitToDrain(): Either[io.circe.Error, (ZonedDateTime, ZonedDateTime)] = {
      val output = GCloud.runGcloud(config, s"dataflow jobs describe $jobId").stdout
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

}
