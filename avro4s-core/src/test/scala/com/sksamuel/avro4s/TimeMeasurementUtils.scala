package com.sksamuel.avro4s

import java.util.concurrent.TimeUnit

object TimeMeasurementUtils {

  def measureTimeInMillis[T](block: => T): (Long, T) = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    val milliseconds = TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS)
    milliseconds -> result
  }

  def describeTime(message: String, millis: Long): String = {
    val seconds = millis / 1000
    s"$message Time: ${seconds}.${millis - seconds * 1000} seconds."
  }

  def logTime[T](message: String, log: String => Unit = println)(block: => T): T = {
    val (millis, result) = measureTimeInMillis(block)
    log(describeTime(message, millis))
    result
  }

}