package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{AvroSchema, Encoder, ImmutableRecord}
import org.scalatest.{Matchers, WordSpec}

case class County(name: String, towns: Seq[Town], ceremonial: Boolean, lat: Double, long: Double)
case class Town(name: String, population: Int)

class StructEncoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "RecordEncoder" should {
    "encode structs" in {
      val countySchema = AvroSchema[County]
      val townSchema = AvroSchema[Town]
      val obj = County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)
      val hardwick = ImmutableRecord(townSchema, Vector("Hardwick", java.lang.Integer.valueOf(123)))
      val weedon = ImmutableRecord(townSchema, Vector("Weedon", java.lang.Integer.valueOf(225)))
      Encoder[County].encode(obj, countySchema) shouldBe ImmutableRecord(countySchema, Vector("Bucks", List(hardwick, weedon).asJava, java.lang.Boolean.valueOf(true), java.lang.Double.valueOf(12.34), java.lang.Double.valueOf(0.123)))
    }
  }
}
