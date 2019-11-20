package com.sksamuel.avro4s.github

import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class Github396 extends FunSuite with Matchers {

  implicit val fieldMapping = SnakeCase

  case class Foo(@AvroName("WIBBLE") aWobble: String, bWubble: String)

  test("@AvroName should override fieldmapping for schema gen") { // Issue #396
    val schema = AvroSchema[Foo]
    val expected = new org.apache.avro.Schema.Parser().parse(
      getClass.getResourceAsStream(
        "/avro_name_field_mapping.json"
      )
    )
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("@AvroName should should override fieldmapping on decoding") {

    val schema = AvroSchema[Foo]

    val obj = Foo("one", "two")

    val record = new GenericData.Record(schema)
    record.put("WIBBLE", "one")
    record.put("b_wubble", "two")


    Decoder[Foo].decode(record, schema, SnakeCase) shouldBe obj
  }

  test("@AvroName should override fieldmapping on encoding") {

    val schema = AvroSchema[Foo]

    val obj = Foo("one", "two")

    val record = ImmutableRecord(schema, Vector(new Utf8("one"), new Utf8("two")))


    Encoder[Foo].encode(obj, schema, SnakeCase) shouldBe record
  }

}
