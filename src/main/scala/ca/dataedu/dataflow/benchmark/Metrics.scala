package ca.dataedu.dataflow.benchmark

object Metrics {

  val TotalVcpuTime = "TotalVcpuTime"
  val TotalMemoryUsage = "TotalMemoryUsage"
  val TotalPdUsage = "TotalPdUsage"
  val TotalSsdUsage = "TotalSsdUsage"
  val TotalShuffleDataProcessed = "TotalShuffleDataProcessed"
  val BillableShuffleDataProcessed = "BillableShuffleDataProcessed"

  val BacklogBytes = "pubsub.googleapis.com/subscription/backlog_bytes"

}
