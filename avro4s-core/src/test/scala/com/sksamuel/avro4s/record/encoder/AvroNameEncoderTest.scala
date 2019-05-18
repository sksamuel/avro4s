package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroName, AvroSchema, Encoder, AvroNamespace}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class AvroNameEncoderTest extends FunSuite with Matchers {

  case class AvroNameEncoderTest(@AvroName("bar") foo: String)

  @AvroNamespace("some.pkg")
  case class AvroNamespaceEncoderTest(foo: String)

  test("encoder should take into account @AvroName") {
    val schema = AvroSchema[AvroNameEncoderTest]
    val record = Encoder[AvroNameEncoderTest].encode(AvroNameEncoderTest("hello"), schema).asInstanceOf[GenericRecord]
    record.get("bar") shouldBe new Utf8("hello")
  }

  test("encoding sealed traits of case objects should take into account AvroName") {
    val schema = AvroSchema[Ship]
    val record = Encoder[Ship].encode(Ship(Atlantic), schema).asInstanceOf[GenericRecord]
    record.get("location").toString shouldBe "atlantic"
  }

  test("encoding sealed traits of case objects should take into account @AvroNamespace") {
    val schema = AvroSchema[WaterproofBox]
    val data = WaterproofBox(AirtightBox(Cucumber(1.23)))
    val record = Encoder[WaterproofBox].encode(data, schema).asInstanceOf[GenericRecord]
    val abox = record.get("airtight_box").asInstanceOf[GenericRecord]
    val contents = abox.get("contents").asInstanceOf[GenericRecord]
    contents.get("length") shouldBe 1.23
  }
}

@AvroNamespace("storage.boxes")
case class WaterproofBox(airtight_box: AirtightBox)

@AvroNamespace("storage.boxes")
case class AirtightBox(contents: Food)

sealed trait Food

@AvroNamespace("storage.boxes")
@AvroName("cucumber")
case class Cucumber(length: Double) extends Food

@AvroNamespace("storage.boxes")
@AvroName("blackberry")
case class Blackberry(colour: String) extends Food

sealed trait Ocean

@AvroName("atlantic")
case object Atlantic extends Ocean

@AvroName("pacific")
case object Pacific extends Ocean
case class Ship(location: Ocean)


