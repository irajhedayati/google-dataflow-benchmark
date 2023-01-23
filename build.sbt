val Organization = "ca.dataedu"
val ApplicationName = "google-dataflow-benchmark"
val ScalaVersion = "2.13.8"
val ScalaBinaryVersion = "2.13"
val RootPackage = "dataflow.benchmark"
val MainClass = s"$Organization.$RootPackage.Main"


lazy val root = (project in file("."))
  .settings(
    Seq(
      organization := Organization,
      name := ApplicationName,
      scalaVersion := ScalaVersion,
      scalaBinaryVersion := ScalaBinaryVersion,
      javacOptions ++= Seq("-source", "11", "-target", "11"),
      scalacOptions ++= Seq("-deprecation"),
//      scalafmtOnCompile := true,
      useCoursier := false
    )
  )
  .settings(Dependencies.dependencies)
