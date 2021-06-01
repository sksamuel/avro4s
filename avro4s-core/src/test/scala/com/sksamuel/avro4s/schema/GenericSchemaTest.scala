//package com.sksamuel.avro4s.schema
//
//import java.util.UUID
//
//import com.sksamuel.avro4s.{AvroErasedName, AvroName, AvroSchema}
//import org.apache.avro.SchemaParseException
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//case class Thing1Id(id: Int) extends AnyVal
//case class Thing2Id(id: UUID) extends AnyVal
//case class Thing2(id: Thing2Id, name: String)
//case class OuterId(id: UUID) extends AnyVal
//case class InnerId(id: String) extends AnyVal
//case class Inner[LEFT, RIGHT](
//  id: InnerId,
//  left: LEFT,
//  right: Option[RIGHT] = None
//)
//case class Outer[LEFT, RIGHT](
//  id: OuterId,
//  @AvroName("innerThing")
//  theInnerThing: Inner[LEFT, RIGHT]
//)
//
//class GenericSchemaTest extends AnyFunSuite with Matchers {
//
//  case class Generic[T](t: T)
//  case class SameGenericWithDifferentTypeArgs(gi: Generic[Int], gs: Generic[String])
//
//  @AvroErasedName
//  case class GenericDisabled[T](t: T)
//  case class SameGenericWithDifferentTypeArgsDisabled(gi: GenericDisabled[Int], gs: GenericDisabled[String])
//
//  test("support same generic with different type parameters") {
//    val schema = AvroSchema[SameGenericWithDifferentTypeArgs]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//
//  test("throw error if different type parameters are disabled by @AvroErasedName") {
//    intercept[SchemaParseException] {
//      val schema = AvroSchema[SameGenericWithDifferentTypeArgsDisabled]
//      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic.json"))
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//  }
//
//  test("support top level generic") {
//    val schema1 = AvroSchema[Generic[String]]
//    val expected1 = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic_string.json"))
//    schema1.toString(true) shouldBe expected1.toString(true)
//
//    val schema2 = AvroSchema[Generic[Int]]
//    val expected2 = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic_int.json"))
//    schema2.toString(true) shouldBe expected2.toString(true)
//  }
//
//  test("generate only raw name if @AvroErasedName is present") {
//    val schema = AvroSchema[GenericDisabled[String]]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/generic_disabled.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//
//  test("support @AvroName on generic type args") {
//    val schema = AvroSchema[DibDabDob]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_name_on_type_arg.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//
//  test("deeply nested generics") {
//    val schema = AvroSchema[Outer[Thing1Id, Option[Seq[Thing2]]]]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/deeply_nested_generics.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//
//  test("deeply nested generic with value type") {
//    val schema = AvroSchema[Generic[Option[Seq[Thing1Id]]]]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/deeply_nested_generic_with_value_type.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//}
//
//object First {
//  case class TypeArg(a: String)
//}
//
//object Second {
//  @AvroName("wibble")
//  case class TypeArg(a: String)
//}
//
//case class Generic[T](value: T)
//
//case class DibDabDob(a: Generic[First.TypeArg], b: Generic[Second.TypeArg])