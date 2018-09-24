package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.{AvroSchema, BinaryType, DataType, DataTypeFor}
import org.scalatest.{FunSuite, Matchers}

class SchemaOverrideTest extends FunSuite with Matchers {

  test("allow overriding built in implicits for a core type") {

    implicit val StringAsBytes = new DataTypeFor[String] {
      override def dataType: DataType = BinaryType
    }

    case class OverrideTest(s: String, i: Int)

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override.json"))
    val schema = AvroSchema[OverrideTest]
    schema.toString(true) shouldBe expected.toString(true)
  }
}
