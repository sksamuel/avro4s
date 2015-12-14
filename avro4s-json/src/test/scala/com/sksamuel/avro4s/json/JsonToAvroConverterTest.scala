package com.sksamuel.avro4s.json

import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class JsonToAvroConverterTest extends WordSpec with Matchers {

  "JsonToAvroConverter" should {
    "convert json to avro" in {
      for (k <- 1 to 2) {
        println(k)
        val json = Source.fromInputStream(getClass.getResourceAsStream(s"/json$k.json")).getLines.mkString("\n")
        val schema = new JsonToAvroConverter("com.test.avro").convert("MyClass", json)
        println(schema.toString(true))
        schema.toString(true) shouldBe new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(s"/avro$k.avsc")).toString(true)
      }
    }
  }
}
