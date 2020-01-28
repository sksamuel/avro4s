package com.sksamuel.avro4s.github

import java.time.LocalTime

import com.sksamuel.avro4s.{Decoder, EncoderV2}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GithubIssue387 extends AnyWordSpec with Matchers {

  val NANOSECONDS_IN_A_MICROSECOND = 1000

  "LocalTime" must {

    "encode the value to a int represented as milliseconds since midnight" in {
      val localTime = LocalTime.now()
      val encoded = EncoderV2[LocalTime].encode(localTime)
      encoded shouldBe localTime.toNanoOfDay / NANOSECONDS_IN_A_MICROSECOND
    }

    "encode the value and truncate any precision beyond milliseconds" in {
      val encoded = EncoderV2[LocalTime].encode(LocalTime.MAX)
      encoded shouldBe LocalTime.MAX.toNanoOfDay / NANOSECONDS_IN_A_MICROSECOND
    }

    "encode and decode back to an equivalent LocalTime object when Local has microsecond precision" in {
      val localTime = LocalTime.now()
      val encoded = EncoderV2[LocalTime].encode(localTime)
      val decoded = Decoder[LocalTime].decode(encoded)
      decoded shouldBe localTime
      decoded.toNanoOfDay shouldBe localTime.toNanoOfDay
    }

    "encode and decode back to a LocalTime object with an equivalent time to  microsecond precision" in {
      val encoded = EncoderV2[LocalTime].encode(LocalTime.MAX)
      val decoded = Decoder[LocalTime].decode(encoded)
      decoded should not be LocalTime.MAX
      // compare to a LocalTime.MAX that has had the time precision truncated to milliseconds
      decoded shouldBe LocalTime.ofNanoOfDay((LocalTime.MAX.toNanoOfDay / NANOSECONDS_IN_A_MICROSECOND) * NANOSECONDS_IN_A_MICROSECOND)
    }

  }

}
