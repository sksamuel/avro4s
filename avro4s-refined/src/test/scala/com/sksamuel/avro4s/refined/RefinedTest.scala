package com.sksamuel.avro4s.refined

import com.sksamuel.avro4s._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Foo(nonEmptyStr: String Refined NonEmpty)

class RefinedTest extends AnyWordSpec with Matchers {

  "refinedSchemaFor" should {
    "use the schema for the underlying type" in {
      AvroSchema[Foo] shouldBe new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "Foo",
          |	"namespace": "com.sksamuel.avro4s.refined",
          |	"fields": [{
          |		"name": "nonEmptyStr",
          |		"type": "string"
          |	}]
          |}
        """.stripMargin)
    }

    "generate correct schemas for a Map when refined instances are in scope" in {
      case class Test(map: Map[String, Int], nonEmptyStr: String Refined NonEmpty)
      val schema = AvroSchema[Test]

      schema.getField("map").schema().getType shouldBe Schema.Type.MAP
      schema.getField("nonEmptyStr").schema().getType shouldBe Schema.Type.STRING
    }
  }

  "refinedEncoder" should {
    "use the encoder for the underlying type" in {
      val expected: String Refined NonEmpty = "foo"
      val record = ToRecord[Foo].to(Foo(expected))
      record.get("nonEmptyStr").toString shouldBe expected.value
    }
  }

  "refinedDecoder" should {
    "use the decoder for the underlying type" in {
      val expected: String Refined NonEmpty = "foo"
      val record = ImmutableRecord(AvroSchema[Foo], Vector(expected.value))
      FromRecord[Foo].from(record) shouldBe Foo(expected)
    }

    "throw when the value does not conform to the refined predicate" in {
      val record = ImmutableRecord(AvroSchema[Foo], Vector(""))
      assertThrows[IllegalArgumentException](FromRecord[Foo].from(record))
    }
  }
}
