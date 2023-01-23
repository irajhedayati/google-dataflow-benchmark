package ca.dataedu.dataflow.benchmark

import com.google.monitoring.v3.TimeInterval
import com.google.protobuf.Timestamp

import java.time.{Duration, ZoneId, ZonedDateTime}

object Time {


  val DataflowMetricDelay: Duration = Duration.ofMinutes(4)

  def nowInUtc: ZonedDateTime = ZonedDateTime.now(ZoneId.of("UTC"))

  def timeInterval(start: ZonedDateTime, end: ZonedDateTime): TimeInterval = TimeInterval
    .newBuilder()
    .setStartTime(Timestamp.newBuilder().setSeconds(start.toEpochSecond).build())
    .setEndTime(Timestamp.newBuilder().setSeconds(end.toEpochSecond).build())
    .build()

}
