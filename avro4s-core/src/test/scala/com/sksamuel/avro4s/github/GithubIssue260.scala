package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroName, AvroSchema}
import org.apache.avro.SchemaNormalization
import org.scalatest.{FunSuite, Matchers}

class GithubIssue260 extends FunSuite with Matchers {
  test("Schema generated is not always the same (#260)") {
    val fingerprints = (1 to 100).map { _ =>
      SchemaNormalization.parsingFingerprint64(AvroSchema[ActivityEvent])
    }.toSet.size shouldBe 1
  }
}

case class ActivityEvent(uuid: String,
                         @AvroName("eventName") eventName: event_name.Value,
                         @AvroName("activityType") activityType: activity_type.Value,
                         activityID: Int,
                         activityRecordIndex: Int,
                         utcTimestampBegin: String,
                         utcTimestampEnd: Option[String] = None,
                         isGpsOn: Option[Boolean] = None,
                         batteryPercentage: Option[Int] = None,
                         locations: Option[Seq[locations]] = None,
                         accelerations: Option[Seq[accelerations]] = None,
                         gyros: Option[Seq[gyros]] = None,
                         magnetometers: Option[Seq[magnetometers]] = None,
                         events: Option[Seq[events]] = None
                        )

@AvroName("EventName") object event_name extends Enumeration {
  val ACTIVITY_START, ACTIVITY_STOP, ACTIVITY_RECORD = Value
}

@AvroName("ActivityType") object activity_type extends Enumeration {
  val IN_VEHICLE, ON_BICYCLE = Value
}
object event_type extends Enumeration {
  val STEP, PICKUP, GLANCE, WAKE_UP, TILT, SCREEN_UNLOCK = Value
}


case class locations(
                      utcTimestamp: String,
                      provider: String,
                      latitude: Float,
                      longitude: Float,
                      altitude: Option[Float] = None,
                      horizontalAccuracy: Option[Float] = None,
                      verticalAccuracy: Option[Float] = None,
                      speed: Option[Float] = None,
                      speedAccuracy: Option[Float] = None,
                      bearing: Option[Float] = None,
                      bearingAccuracy: Option[Long] = None
                    )

case class accelerations(
                          x: Float,
                          y: Float,
                          z: Float,
                          norm: Float,
                          accuracy: Int,
                          utcTimestamp: String
                        )

case class gyros(
                  x: Float,
                  y: Float,
                  z: Float,
                  accuracy: Int,
                  utcTimestamp: String
                )

case class magnetometers(
                          x: Float,
                          y: Float,
                          z: Float,
                          accuracy: Int,
                          utcTimestamp: String
                        )

case class events(
                   utcTimestamp: String,
                   @AvroName("event_type") eventType: event_type.Value,
                   magnitude: Option[Float] = None,
                   severity: Option[Int] = None,
                   accuracy: Option[Int] = None
                 )
