package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{AvroSchema, Encoder}
import org.apache.avro.generic.GenericRecord
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
}

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
