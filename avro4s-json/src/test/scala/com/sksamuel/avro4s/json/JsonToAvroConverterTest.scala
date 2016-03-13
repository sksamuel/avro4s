package com.sksamuel.avro4s.json

import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class JsonToAvroConverterTest extends WordSpec with Matchers {

  "JsonToAvroConverter" should {
    "convert json to avro" in {
      for (k <- 1 to 2) {
        val json = Source.fromInputStream(getClass.getResourceAsStream(s"/json$k.json")).getLines.mkString("\n")
        val schema = new JsonToAvroConverter("com.test.avro").convert("MyClass", json)
        schema.toString(true) shouldBe new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(s"/avro$k.avsc")).toString(true)
      }
    }

    "convert json to avro but avoid utf8 and set the schema to use String" in {
      for (k <- 1 to 2) {
        val json = Source.fromInputStream(getClass.getResourceAsStream(s"/json$k.json")).getLines.mkString("\n")
        val schema = new JsonToAvroConverter("com.test.avro", true).convert("MyClass", json)
        schema.toString(true) shouldBe new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(s"/avro${k}_with_strings.avsc")).toString(true)
      }
    }

    "convert nulls to Option[String]" in {
      val json = """ { "foo" : null } """
      val schema = new JsonToAvroConverter("com.test.avro").convert("MyClass", json)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/null.avsc")).toString(true)
      schema.toString(true) shouldBe expected
    }
    "convert Arrays to List[X]" in {
      val json = """ { "foo" : [ true, false, true ] } """
      val schema = new JsonToAvroConverter("com.test.avro").convert("MyClass", json)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arraysbooleans.avsc")).toString(true)
      schema.toString(true) shouldBe expected
    }
  }
}
