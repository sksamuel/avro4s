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
      AvroSchemaV2[Foo] shouldBe new Schema.Parser().parse(
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
      val record = ImmutableRecord(AvroSchemaV2[Foo], Vector(expected.value))
      FromRecord[Foo].from(record) shouldBe Foo(expected)
    }

    "throw when the value does not conform to the refined predicate" in {
      val record = ImmutableRecord(AvroSchemaV2[Foo], Vector(""))
      assertThrows[IllegalArgumentException](FromRecord[Foo].from(record))
    }
  }
}
