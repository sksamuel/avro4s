package com.sksamuel.avro4s.github

import java.time.LocalTime
import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GithubIssue387 extends AnyWordSpec with Matchers {

  val NANOSECONDS_IN_A_MICROSECOND = 1000

  "LocalTime" must {

    "encode the value to a long represented as nanoseconds since midnight" in {
      val localTime = LocalTime.now()
      val encoded = Encoder[LocalTime].encode(AvroSchema[LocalTime]).apply(localTime)
      encoded shouldBe localTime.toNanoOfDay
    }

    "encode and decode back to an equivalent LocalTime object when Local has nanosecond precision" in {
      val localTime = LocalTime.now()
      val encoded = Encoder[LocalTime].encode(AvroSchema[LocalTime]).apply(localTime)
      val decoded = Decoder[LocalTime].decode(AvroSchema[LocalTime]).apply(encoded)
      decoded shouldBe localTime
      decoded.toNanoOfDay shouldBe localTime.toNanoOfDay
    }

    "encode and decode back to a LocalTime object with an equivalent time to nanosecond precision" in {
      val encoded = Encoder[LocalTime].encode(AvroSchema[LocalTime]).apply(LocalTime.MAX)
      val decoded = Decoder[LocalTime].decode(AvroSchema[LocalTime]).apply(encoded)
      decoded shouldBe LocalTime.MAX
      // compare to a LocalTime.MAX that has nanosecond precision
      decoded shouldBe LocalTime.ofNanoOfDay(LocalTime.MAX.toNanoOfDay)
    }

  }

}
