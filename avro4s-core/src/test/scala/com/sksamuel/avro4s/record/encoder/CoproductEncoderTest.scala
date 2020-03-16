package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.schema.Colours
import com.sksamuel.avro4s.{AvroSchema, DefaultFieldMapper, Encoder, ImmutableRecord}
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.util.Utf8
import shapeless.{:+:, CNil, Coproduct}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CoproductEncoderTest extends AnyFunSuite with Matchers {

  test("coproducts with primitives") {
    val schema = AvroSchema[CPWrapper]
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG](4)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(4)))
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG]("wibble")), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new Utf8("wibble")))
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG](true)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true)))
  }

  test("coproducts with case classes") {
    val schema = AvroSchema[CPWrapper]
    val gschema = AvroSchema[Gimble]
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo"))), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(ImmutableRecord(gschema, Vector(new Utf8("foo")))))
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

  test("coproducts with enums") {
    type ColorOrNil = Colours.Value :+: CNil
    val schema = AvroSchema[CoproductOfEnum]
    Encoder[CoproductOfEnum].encode(CoproductOfEnum(Coproduct[ColorOrNil](Colours.Red)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("union").schema(), "Red")))
  }
  
  test("coproducts with enum and Int") {
    type ColorOrInt = Colours.Value :+: Int :+: CNil
    val schema = AvroSchema[CoproductOfEnumOrInt]
    Encoder[CoproductOfEnumOrInt].encode(CoproductOfEnumOrInt(Coproduct[ColorOrInt](4)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(4)))
    Encoder[CoproductOfEnumOrInt].encode(CoproductOfEnumOrInt(Coproduct[ColorOrInt](Colours.Red)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("union").schema(), "Red")))
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

case class CoproductOfEnum(union: Colours.Value :+: CNil)
case class CoproductOfEnumOrInt(union: Colours.Value :+: Int :+: CNil)