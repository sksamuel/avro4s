package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{Avro4sException, AvroSchema, Decoder, Encoder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}

class SealedTraitEncoderTest extends AnyFunSuite with Matchers {

  test("support sealed traits of case classes") {
    val schema = AvroSchema[Wrapper]
    val record = Encoder[Wrapper].encode(schema).apply(Wrapper(Wobble("foo"))).asInstanceOf[GenericRecord]
    val wibble = record.get("wibble").asInstanceOf[GenericRecord]
    wibble.get("str") shouldBe new Utf8("foo")
    // the schema should be of the actual impl class
    wibble.getSchema shouldBe AvroSchema[Wobble]
  }

  test("support trait subtypes that have some common field names") {
    val schema = AvroSchema[Trapper]
    val record = Encoder[Trapper].encode(schema).apply(Trapper(Tobble("foo", "bar"))).asInstanceOf[GenericRecord]
    val tobble = record.get("tibble").asInstanceOf[GenericRecord]
    tobble.get("str") shouldBe new Utf8("foo")
    tobble.get("place") shouldBe new Utf8("bar")
    tobble.getSchema shouldBe AvroSchema[Tobble]
  }

  test("support trait subtypes that have some common field names and types") {
    val schema = AvroSchema[Napper]
    val record = Encoder[Napper].encode(schema).apply(Napper(Nabble("foo", 44))).asInstanceOf[GenericRecord]
    val nobble = record.get("nibble").asInstanceOf[GenericRecord]
    nobble.get("str").asInstanceOf[Utf8] shouldBe new Utf8("foo")
    nobble.get("age").asInstanceOf[Int] shouldBe 44
    nobble.getSchema shouldBe AvroSchema[Nabble]
  }

  test("support top level sealed traits") {
    val schema = AvroSchema[Nibble]
    val record = Encoder[Nibble].encode(schema).apply(Nabble("foo", 44)).asInstanceOf[GenericRecord]
    record.get("str").asInstanceOf[Utf8] shouldBe new Utf8("foo")
    record.get("age").asInstanceOf[Int] shouldBe 44
    record.getSchema shouldBe AvroSchema[Nabble]
  }

  test("support sealed trait enums") {
    val schema = AvroSchema[DibbleWrapper]
    Encoder[DibbleWrapper].encode(schema).apply(DibbleWrapper(Dobble)).asInstanceOf[GenericRecord].get("dibble") shouldBe new GenericData.EnumSymbol(schema, Dobble)
    Encoder[DibbleWrapper].encode(schema).apply(DibbleWrapper(Dabble)).asInstanceOf[GenericRecord].get("dibble") shouldBe new GenericData.EnumSymbol(schema, Dabble)
  }

  test("support top level sealed trait enums") {
    val schema = AvroSchema[Dibble]
    Encoder[Dibble].encode(schema).apply(Dobble) shouldBe new GenericData.EnumSymbol(schema, Dobble)
    Encoder[Dibble].encode(schema).apply(Dabble) shouldBe new GenericData.EnumSymbol(schema, Dabble)
  }

  test("support sealed traits that contain nested classes") {
    sealed trait Inner
    case class InnerOne(value: Double) extends Inner
    case class InnerTwo(height: Double) extends Inner
    case class Outer(inner: Inner)
    val schema = AvroSchema[Outer]
    val record = Encoder[Outer].encode(schema).apply(Outer(InnerTwo(1.23))).asInstanceOf[GenericRecord]
    val inner = record.get("inner").asInstanceOf[GenericRecord]
    inner.get("height").asInstanceOf[Double] shouldBe 1.23
  }

  test("support sealed traits that contain nested classes that contain options") {
    case class Outer(inner: Inner)
    sealed trait Inner
    case class InnerOne(value: Double, optVal: Option[Float]) extends Inner
    case class InnerTwo(height: Double) extends Inner
    val schema = AvroSchema[Outer]
    val record = Encoder[Outer].encode(schema).apply(Outer(InnerOne(1.23, None))).asInstanceOf[GenericRecord]
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

