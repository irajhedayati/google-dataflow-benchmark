package ca.dataedu.dataflow.benchmark

import com.google.cloud.monitoring.v3.MetricServiceClient
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, SubscriptionAdminSettings}
import com.google.pubsub.v1.{CreateSnapshotRequest, SeekRequest, SnapshotName}
import com.typesafe.scalalogging.LazyLogging

import java.io.{File, PrintWriter}
import scala.language.postfixOps
import scala.util.{Failure, Random, Success, Try}

object Main extends LazyLogging {

  private val subAdmin: SubscriptionAdminClient = SubscriptionAdminClient.create(
    SubscriptionAdminSettings
      .create(SubscriberStubSettings.newBuilder().build())
  )

  private val metricServiceClient = MetricServiceClient.create()

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

    val analyzer = new Analyze(config, metricServiceClient)
    val runner = new Benchmark(config, metricServiceClient)

    Try(
      // run all the benchmarks
      config.benchmarks
        .foreach { benchmark =>
          logger.info(s"Starting benchmark ${benchmark.name}")
          val snapShotName = prepare(config)
          runner.run(benchmark).map { case (jobId, created, finished) =>
            logger.info("Start analyzing the metrics")
            analyzer.analyze(benchmark, jobId, created, finished, outputWriter)
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
