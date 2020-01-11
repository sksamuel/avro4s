package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Codec, DefaultFieldMapper, Encoder, FieldMapper, ImmutableRecord}
import org.apache.avro.util.Utf8
import shapeless.{:+:, CNil, Coproduct}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CoproductEncoderTest extends AnyFunSuite with Matchers {

  test("coproducts with primitives") {
    implicit val fieldMapper: FieldMapper = DefaultFieldMapper
    val codec = Codec[CPWrapper]
    codec.encode(CPWrapper(Coproduct[CPWrapper.ISBG](4))) shouldBe ImmutableRecord(codec.schema, Vector(java.lang.Integer.valueOf(4)))
    codec.encode(CPWrapper(Coproduct[CPWrapper.ISBG]("wibble"))) shouldBe ImmutableRecord(codec.schema, Vector(new Utf8("wibble")))
    codec.encode(CPWrapper(Coproduct[CPWrapper.ISBG](true))) shouldBe ImmutableRecord(codec.schema, Vector(java.lang.Boolean.valueOf(true)))
  }

  test("coproducts with case classes") {
    implicit val fieldMapper: FieldMapper = DefaultFieldMapper
    val gschema = Codec[Gimble].schema
    val codec = Codec[CPWrapper]
    codec.encode(CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo")))) shouldBe ImmutableRecord(codec.schema, Vector(ImmutableRecord(gschema, Vector(new Utf8("foo")))))
  }

  test("options of coproducts") {
    val schema = AvroSchema[CPWithOption]
    Encoder[CPWithOption].encode(CPWithOption(Some(Coproduct[CPWrapper.ISBG]("foo"))), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new Utf8("foo")))
    Encoder[CPWithOption].encode(CPWithOption(None), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(null))
  }

  test("coproducts with arrays") {
    val schema = AvroSchema[CPWithArray]
    Encoder[CPWithArray].encode(CPWithArray(Coproduct[CPWrapper.SSI](Seq("foo", "bar"))), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.util.Arrays.asList(new Utf8("foo"), new Utf8("bar"))))
    Encoder[CPWithArray].encode(CPWithArray(Coproduct[CPWrapper.SSI](4)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(4)))
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
