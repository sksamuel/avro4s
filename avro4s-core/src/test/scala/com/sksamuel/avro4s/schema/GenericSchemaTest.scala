package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroErasedName, AvroName, AvroSchemaV2}
import org.apache.avro.SchemaParseException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GenericSchemaTest extends AnyFunSuite with Matchers {

  case class Generic[T](t: T)
  case class SameGenericWithDifferentTypeArgs(gi: Generic[Int], gs: Generic[String])

  @AvroErasedName
  case class GenericDisabled[T](t: T)
  case class SameGenericWithDifferentTypeArgsDisabled(gi: GenericDisabled[Int], gs: GenericDisabled[String])

  test("support same generic with different type parameters") {
    val schema = AvroSchemaV2[SameGenericWithDifferentTypeArgs]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("throw error if different type parameters are disabled by @AvroErasedName") {
    intercept[SchemaParseException] {
      val schema = AvroSchemaV2[SameGenericWithDifferentTypeArgsDisabled]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }

  test("support top level generic") {
    val schema1 = AvroSchemaV2[Generic[String]]
    val expected1 = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic_string.json"))
    schema1.toString(true) shouldBe expected1.toString(true)

    val schema2 = AvroSchemaV2[Generic[Int]]
    val expected2 = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic_int.json"))
    schema2.toString(true) shouldBe expected2.toString(true)
  }

  test("generate only raw name if @AvroErasedName is present") {
    val schema = AvroSchemaV2[GenericDisabled[String]]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic_disabled.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support @AvroName on generic type args") {
    val schema = AvroSchemaV2[DibDabDob]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_name_on_type_arg.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

object First {
  case class TypeArg(a: String)
}

object Second {
  @AvroName("wibble")
  case class TypeArg(a: String)
}

case class Generic[T](value: T)

case class DibDabDob(a: Generic[First.TypeArg], b: Generic[Second.TypeArg])