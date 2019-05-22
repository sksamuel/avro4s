package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultNamingStrategy}
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

case class OptionCounty(county: Option[County])
case class County(name: String, towns: Seq[Town], ceremonial: Boolean, lat: Double, long: Double)
case class Town(name: String, population: Int)

class StructDecoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Decoder" should {
    "decode structs" in {

      val countySchema = AvroSchema[County]
      val townSchema = AvroSchema[Town]

      val obj = County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)

      val hardwick = new GenericData.Record(townSchema)
      hardwick.put("name", "Hardwick")
      hardwick.put("population", 123)

      val weedon = new GenericData.Record(townSchema)
      weedon.put("name", "Weedon")
      weedon.put("population", 225)

      val bucks = new GenericData.Record(countySchema)
      bucks.put("name", "Bucks")
      bucks.put("towns", List(hardwick, weedon).asJava)
      bucks.put("ceremonial", true)
      bucks.put("lat", 12.34)
      bucks.put("long", 0.123)

      Decoder[County].decode(bucks, countySchema, DefaultNamingStrategy) shouldBe obj
    }

    "decode optional structs" in {
      val countySchema = AvroSchema[County]
      val townSchema = AvroSchema[Town]
      val optionCountySchema = AvroSchema[OptionCounty]

      val obj = OptionCounty(Some(County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)))

      val hardwick = new GenericData.Record(townSchema)
      hardwick.put("name", "Hardwick")
      hardwick.put("population", 123)

      val weedon = new GenericData.Record(townSchema)
      weedon.put("name", "Weedon")
      weedon.put("population", 225)

      val bucks = new GenericData.Record(countySchema)
      bucks.put("name", "Bucks")
      bucks.put("towns", List(hardwick, weedon).asJava)
      bucks.put("ceremonial", true)
      bucks.put("lat", 12.34)
      bucks.put("long", 0.123)

      val record = new GenericData.Record(optionCountySchema)
      record.put("county", bucks)

      Decoder[OptionCounty].decode(record, optionCountySchema, DefaultNamingStrategy) shouldBe obj

      val emptyRecord = new GenericData.Record(optionCountySchema)
      emptyRecord.put("county", null)

      Decoder[OptionCounty].decode(emptyRecord, optionCountySchema, DefaultNamingStrategy) shouldBe OptionCounty(None)
    }
  }
}
