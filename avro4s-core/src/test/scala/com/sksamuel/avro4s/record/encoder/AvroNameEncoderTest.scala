package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema, Decoder, DefaultNamingStrategy, Encoder, ImmutableRecord, SchemaFor, ToRecord}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class AvroNameEncoderTest extends FunSuite with Matchers {

  case class AvroNameEncoderTest(@AvroName("bar") foo: String)

  @AvroNamespace("some.pkg")
  case class AvroNamespaceEncoderTest(foo: String)

  test("encoder should take into account @AvroName on a field") {
    val schema = AvroSchema[AvroNameEncoderTest]
    val record = Encoder[AvroNameEncoderTest].encode(AvroNameEncoderTest("hello"), schema, DefaultNamingStrategy).asInstanceOf[GenericRecord]
    record.get("bar") shouldBe new Utf8("hello")
  }

  test("encoding sealed traits of case objects should take into account AvroName") {
    val schema = AvroSchema[Ship]
    val record = Encoder[Ship].encode(Ship(Atlantic), schema, DefaultNamingStrategy).asInstanceOf[GenericRecord]
    record.get("location").toString shouldBe "atlantic"
  }

  test("encoding sealed traits of case objects should take into account @AvroNamespace") {
    val schema = AvroSchema[WaterproofBox]
    val data = WaterproofBox(AirtightBox(Cucumber(1.23)))
    val record = Encoder[WaterproofBox].encode(data, schema, DefaultNamingStrategy).asInstanceOf[GenericRecord]
    val abox = record.get("airtight_box").asInstanceOf[GenericRecord]
    val contents = abox.get("contents").asInstanceOf[GenericRecord]
    contents.get("length") shouldBe 1.23
  }

  test("support encoding and decoding with empty namespaces") {
    val spaceship = Spaceship(MiserableCosmos(true))
    val encoded = Encoder[Spaceship].encode(spaceship, SchemaFor[Spaceship].schema(DefaultNamingStrategy), DefaultNamingStrategy)
    val decoded = Decoder[Spaceship].decode(encoded, SchemaFor[Spaceship].schema(DefaultNamingStrategy), DefaultNamingStrategy)
    spaceship shouldBe decoded
  }

  test("encoding sealed traits with @AvroNamespace at the field level should work #255") {
    val schema = AvroSchema[MyStark]
    val ms = MyStark(Sansa(1), "", 0)
    val record = ToRecord[MyStark](schema).to(ms).asInstanceOf[ImmutableRecord]

    val sansa = SchemaBuilder.record("Sansa").namespace("the.north").fields().requiredInt("i").endRecord()
    val bran = SchemaBuilder.record("Bran").namespace("the.north").fields().requiredString("s").endRecord()

    record.getSchema shouldBe SchemaBuilder.record("MyStark").namespace("com.sksamuel.avro4s.record.encoder")
      .fields()
      .name("stark").`type`(SchemaBuilder.unionOf().`type`(sansa).and().`type`(bran).endUnion()).noDefault()
      .requiredString("id")
      .requiredInt("x")
      .endRecord()
    record.values.size shouldBe 3
    record.values.head.asInstanceOf[ImmutableRecord].schema shouldBe sansa
    record.values.head.asInstanceOf[ImmutableRecord].values shouldBe Vector(1)
  }
}

sealed trait Stark
case class Sansa(i: Int) extends Stark
case class Bran(s: String) extends Stark

case class MyStark(@AvroNamespace("the.north") stark: Stark, id: String, x: Int)

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

@AvroNamespace("")
case class Spaceship(cosmos: Cosmos)
@AvroNamespace("")
sealed trait Cosmos
@AvroNamespace("")
case class FunCosmos(amountOfFun: Float) extends Cosmos
@AvroNamespace("")
case class MiserableCosmos(isTrulyAwful: Boolean) extends Cosmos
