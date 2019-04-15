package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchemaMerge
import org.apache.avro.SchemaBuilder
import org.scalatest.{Matchers, WordSpec}

class AvroSchemaMergeTest extends WordSpec with Matchers {
  "AvroSchemaMerge" should {
    "merge schemas with union type" in {
      val schemaOne = SchemaBuilder
        .builder("test")
        .record("s1")
        .fields()
        .requiredString("f1")
        .nullableLong("f2", 0)
        .endRecord()

      val schemaTwo = SchemaBuilder
        .builder("test")
        .record("s2")
        .fields()
        .optionalString("f1")
        .requiredLong("f2")
        .endRecord()

      val expected = SchemaBuilder
        .builder("test")
        .record("s3")
        .fields()
        .optionalString("f1")
        .nullableLong("f2", 0)
        .endRecord()

      AvroSchemaMerge.apply("s3", "test", List(schemaOne, schemaTwo)).toString shouldBe expected.toString
    }
  }
}
