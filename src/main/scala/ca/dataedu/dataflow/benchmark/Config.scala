package ca.dataedu.dataflow.benchmark

import com.google.pubsub.v1.SubscriptionName
import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigConvert.catchReadError
import pureconfig._
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.auto._

object Config extends LazyLogging {
  implicit val subscriptionReader: ConfigReader[SubscriptionName] =
    ConfigReader.fromString[SubscriptionName](catchReadError(SubscriptionName.parse))

  def apply(applicationName: String): Config =
    ConfigSource.default.at(applicationName).load[Config] match {
      case Left(failures: ConfigReaderFailures) =>
        logger.error(s"Unable to load configuration:\n${failureString(failures)}")
        sys.exit(1)
      case Right(conf: Config) => conf
    }

  private def failureString(failures: ConfigReaderFailures): String =
    failures.toList.map(f => s"${f.description} at [${f.origin}]").mkString("\n")
}

/** @param jarFile
  *   JAR file that contains the Dataflow application
  * @param className
  *   the main class name in the JAR file
  * @param gcpBucket
  *   a GCS bucket to use for temp and staging locations
  * @param project
  *   the GCP project
  * @param region
  *   the region to run the job
  * @param benchmarks
  *   a list of benchmark definitions
  * @param defaultWorkerLogLevel
  *   log level of workers
  * @param inputPubSubSubscription
  *   the Pub/Sub subscription that is used as input to the job
  * @param outputTransforms
  *   a list of Apache Beam transform name that generate output. This is used to get throughput metrics
  * @param gcloudPath
  *   the path to `gcloud` binary on the orchestrator machine to communicate with Google services
  * @param numberOfRecords
  *   total number of records in your test bed; this is used to estimate the pipeline throughput
  * @param outputFile
  *   a path to generate the report in CSV
  */
case class Config(
    jarFile: String,
    className: Option[String],
    gcpBucket: String,
    project: String,
    region: String,
    benchmarks: List[BenchmarkConfig],
    defaultWorkerLogLevel: String,
    inputPubSubSubscription: SubscriptionName,
    outputTransforms: List[String],
    gcloudPath: String,
    numberOfRecords: Long,
    outputFile: String
)

/**
 *
 * @param name the name of the benchmark that is used in logs and final report
 * @param machineType the machine type to use as Dataflow worker
 * @param numberOfWorkers the number of workers
 * @param maxNumberOfWorkers the maximum number of workers
 * @param diskSizeGb the disk size for worker in GB
 * @param applicationArgs the command line arguments to pass to the application
 */
case class BenchmarkConfig(
    name: String,
    machineType: String,
    numberOfWorkers: Int,
    maxNumberOfWorkers: Int,
    diskSizeGb: Int,
    applicationArgs: List[String]
)
