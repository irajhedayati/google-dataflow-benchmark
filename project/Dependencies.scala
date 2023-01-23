import sbt._
import sbt.Keys._

object Dependencies {

  val CirceVersion = "0.14.3"
  val EnumeratumCirceVersion = "1.7.2"
  val GoogleCloudMonitoringVersion = "3.8.0"
  val GooglePubsubVersion = "1.123.0"
  val LogbackVersion = "1.4.5"
  val PureConfigVersion = "0.17.2"
  val ScalaLoggingVersion = "3.9.5"
  val ScalaMockVersion = "5.2.0"
  val ScalaTestVersion = "3.2.15"

  lazy val CoreDependencies: Seq[ModuleID] = Seq(
    "com.github.pureconfig"      %% "pureconfig"            % PureConfigVersion,
    "com.github.pureconfig"      %% "pureconfig-enumeratum" % PureConfigVersion,
    "com.beachape"               %% "enumeratum-circe"      % EnumeratumCirceVersion,
    "com.typesafe.scala-logging" %% "scala-logging"         % ScalaLoggingVersion,
    "ch.qos.logback"              % "logback-classic"       % LogbackVersion,
    "org.scalatest"              %% "scalatest"             % ScalaTestVersion % Test,
    "org.scalamock"              %% "scalamock"             % ScalaMockVersion % Test
  )

  lazy val GcpDependencies: Seq[ModuleID] = Seq(
    "com.google.cloud" % "google-cloud-pubsub"          % GooglePubsubVersion,
    "com.google.cloud" % "google-cloud-monitoring"      % GoogleCloudMonitoringVersion
  )
  lazy val CirceDependencies: Seq[ModuleID] = Seq(
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion
  )

  lazy val dependencies =
    libraryDependencies ++= CoreDependencies ++ GcpDependencies ++ CirceDependencies
}
