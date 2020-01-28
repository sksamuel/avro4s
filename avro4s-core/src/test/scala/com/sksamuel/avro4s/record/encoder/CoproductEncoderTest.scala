package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s._
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import shapeless.{:+:, CNil, Coproduct}

class CoproductEncoderTest extends AnyFunSuite with Matchers {

  test("coproducts with primitives") {
    implicit val fieldMapper: FieldMapper = DefaultFieldMapper
    val encoder = Encoder[CPWrapper]
    encoder.encode(CPWrapper(Coproduct[CPWrapper.ISBG](4))) shouldBe ImmutableRecord(encoder.schema, Vector(java.lang.Integer.valueOf(4)))
    encoder.encode(CPWrapper(Coproduct[CPWrapper.ISBG]("wibble"))) shouldBe ImmutableRecord(encoder.schema, Vector(new Utf8("wibble")))
    encoder.encode(CPWrapper(Coproduct[CPWrapper.ISBG](true))) shouldBe ImmutableRecord(encoder.schema, Vector(java.lang.Boolean.valueOf(true)))
  }

  test("coproducts with case classes") {
    implicit val fieldMapper: FieldMapper = DefaultFieldMapper
    val gschema = AvroSchemaV2[Gimble]
    val encoder = Encoder[CPWrapper]
    encoder.encode(CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo")))) shouldBe ImmutableRecord(encoder.schema, Vector(ImmutableRecord(gschema, Vector(new Utf8("foo")))))
  }

  test("options of coproducts") {
    val schema = AvroSchemaV2[CPWithOption]
    Encoder[CPWithOption].encode(CPWithOption(Some(Coproduct[CPWrapper.ISBG]("foo")))) shouldBe ImmutableRecord(schema, Vector(new Utf8("foo")))
    Encoder[CPWithOption].encode(CPWithOption(None)) shouldBe ImmutableRecord(schema, Vector(null))
  }

  test("coproducts with arrays") {
    val schema = AvroSchemaV2[CPWithArray]
    Encoder[CPWithArray].encode(CPWithArray(Coproduct[CPWrapper.SSI](Seq("foo", "bar")))) shouldBe ImmutableRecord(schema, Vector(java.util.Arrays.asList(new Utf8("foo"), new Utf8("bar"))))
    Encoder[CPWithArray].encode(CPWithArray(Coproduct[CPWrapper.SSI](4))) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(4)))
  }
}

case class CPWithArray(u: CPWrapper.SSI)

case class Gimble(x: String)
case class CPWrapper(u: CPWrapper.ISBG)
case class CPWithOption(u: Option[CPWrapper.ISBG])

object CPWrapper {
  type ISBG = Int :+: String :+: Boolean :+: Gimble :+: CNil
  type SSI = Seq[String] :+: Int :+: CNil
}

case class Coproducts(union: Int :+: String :+: Boolean :+: CNil)
case class CoproductsOfCoproducts(union: (Int :+: String :+: CNil) :+: Boolean :+: CNil)
