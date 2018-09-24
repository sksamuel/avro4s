package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, SchemaEncoder}
import com.sksamuel.avro4s.record.decoder.County
import org.scalatest.{Matchers, WordSpec}

case class County(name: String, towns: Seq[Town], ceremonial: Boolean, lat: Double, long: Double)
case class Town(name: String, population: Int)

class StructEncoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "RecordEncoder" should {
    "encode structs" in {
      val countySchema = SchemaEncoder[County].encode()
      val townSchema = SchemaEncoder[Town].encode()
      val obj = County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)
      val hardwick = InternalRecord(townSchema, Vector("Hardwick", java.lang.Integer.valueOf(123)))
      val weedon = InternalRecord(townSchema, Vector("Weedon", java.lang.Integer.valueOf(225)))
      Encoder[County].encode(obj, countySchema) shouldBe InternalRecord(countySchema, Vector("Bucks", List(hardwick, weedon).asJava, java.lang.Boolean.valueOf(true), java.lang.Double.valueOf(12.34), java.lang.Double.valueOf(0.123)))
    }
  }
}
