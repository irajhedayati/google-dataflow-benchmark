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

case class BenchmarkConfig(
    name: String,
    machineType: String,
    numberOfWorkers: Int,
    maxNumberOfWorkers: Int,
    diskSizeGb: Int,
    applicationArgs: List[String]
)
