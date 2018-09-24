package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{AvroSchema, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

case class County(name: String, towns: Seq[Town], ceremonial: Boolean, lat: Double, long: Double)
case class Town(name: String, population: Int)

class StructEncoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "RecordEncoder" should {
    "encode structs" in {
      val countySchema = AvroSchema[County]
      val townSchema = AvroSchema[Town]
      val count = County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)
      val result = Encoder[County].encode(count, countySchema)

      val hardwick = ImmutableRecord(townSchema, Vector(new Utf8("Hardwick"), java.lang.Integer.valueOf(123)))
      val weedon = ImmutableRecord(townSchema, Vector(new Utf8("Weedon"), java.lang.Integer.valueOf(225)))
      result shouldBe ImmutableRecord(countySchema, Vector(new Utf8("Bucks"), List(hardwick, weedon).asJava, java.lang.Boolean.valueOf(true), java.lang.Double.valueOf(12.34), java.lang.Double.valueOf(0.123)))
    }
  }
}
