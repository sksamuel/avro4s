package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.Encoder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class SealedTraitEncoderTest extends FunSuite with Matchers {

  test("support sealed traits of case classes") {
    val schema = AvroSchema[Wrapper]
    val record = Encoder[Wrapper].encode(Wrapper(Wobble("foo")), schema).asInstanceOf[GenericRecord]
    val wibble = record.get("wibble").asInstanceOf[GenericRecord]
    wibble.get("str") shouldBe new Utf8("foo")
    // the schema should be of the actual impl class
    wibble.getSchema shouldBe AvroSchema[Wobble]
  }

  test("support trait subtypes fields with same name") {
    val schema = AvroSchema[Trapper]
    val record = Encoder[Trapper].encode(Trapper(Tobble("foo", "bar")), schema).asInstanceOf[GenericRecord]
    val tobble = record.get("tibble").asInstanceOf[GenericRecord]
    tobble.get("str") shouldBe new Utf8("foo")
    tobble.get("place") shouldBe new Utf8("bar")
    tobble.getSchema shouldBe AvroSchema[Tobble]
  }

  test("support trait subtypes fields with same name and same type") {
    val schema = AvroSchema[Napper]
    val record = Encoder[Napper].encode(Napper(Nabble("foo", 44)), schema).asInstanceOf[GenericRecord]
    val nobble = record.get("nibble").asInstanceOf[GenericRecord]
    nobble.get("str") shouldBe new Utf8("foo")
    nobble.get("age") shouldBe 44
    nobble.getSchema shouldBe AvroSchema[Nabble]
  }

  test("support top level ADTs") {
    val schema = AvroSchema[Nibble]
    val record = Encoder[Nibble].encode(Nabble("foo", 44), schema).asInstanceOf[GenericRecord]
    record.get("str") shouldBe new Utf8("foo")
    record.get("age") shouldBe 44
    record.getSchema shouldBe AvroSchema[Nabble]
  }

  test("trait of case objects should be encoded as enum") {
    val schema = AvroSchema[DibbleWrapper]
    Encoder[DibbleWrapper].encode(DibbleWrapper(Dobble), schema).asInstanceOf[GenericRecord].get("dibble") shouldBe new GenericData.EnumSymbol(schema, Dobble)
    Encoder[DibbleWrapper].encode(DibbleWrapper(Dabble), schema).asInstanceOf[GenericRecord].get("dibble") shouldBe new GenericData.EnumSymbol(schema, Dabble)
  }

  test("top level traits of case objects should be encoded as enum") {
    val schema = AvroSchema[Dibble]
    Encoder[Dibble].encode(Dobble, schema) shouldBe new GenericData.EnumSymbol(schema, Dobble)
    Encoder[Dibble].encode(Dabble, schema) shouldBe new GenericData.EnumSymbol(schema, Dabble)
  }

  test("options of sealed traits should be encoded correctly") {
    val schema = AvroSchema[MeasurableThing]
    val record = Encoder[MeasurableThing].encode(MeasurableThing(Some(WidthDimension(1.23))), schema).asInstanceOf[GenericRecord]
    val width = record.get("dimension").asInstanceOf[GenericRecord]
    width.get("width") shouldBe 1.23
  }
}

sealed trait Dibble
case object Dobble extends Dibble
case object Dabble extends Dibble
case class DibbleWrapper(dibble: Dibble)

sealed trait Wibble
case class Wobble(str: String) extends Wibble
case class Wabble(dbl: Double) extends Wibble
case class Wrapper(wibble: Wibble)

sealed trait Tibble
case class Tobble(str: String, place: String) extends Tibble
case class Tabble(str: Double, age: Int) extends Tibble
case class Trapper(tibble: Tibble)

sealed trait Nibble
case class Nobble(str: String, place: String) extends Nibble
case class Nabble(str: String, age: Int) extends Nibble
case class Napper(nibble: Nibble)

sealed trait Dimension
case class HeightDimension(height: Double) extends Dimension
case class WidthDimension(width: Double) extends Dimension
case class MeasurableThing(dimension: Option[Dimension])

