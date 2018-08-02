package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

class AvroNameTest extends WordSpec with Matchers {

  case class Foo(@AvroName("wibble") wobble: String, wubble: String)

  "ToSchema" should {
    "generate field names using @AvroName" in {
      val schema = SchemaFor[Foo]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avroname.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }

  "ToRecord" should {
    "correctly be able to produce a record" in {
      val toRecord = ToRecord[Foo]
      toRecord(Foo("woop", "scoop"))
    }
  }

  "FromRecord" should {
    "correctly be able to produce a record" in {
      val fromRecord = FromRecord[Foo]
      val record = ToRecord[Foo](Foo("woop", "scoop"))

      fromRecord(record)
    }
  }
}
