package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.schema.Colours
import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.util.Utf8
import shapeless.{:+:, CNil, Coproduct}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CoproductDecoderTest extends AnyFunSuite with Matchers {

  import com.sksamuel.avro4s.schema.Colours

  test("coproducts with primitives") {
    val schema = AvroSchema[CPWrapper]
    val record = new GenericData.Record(schema)
    record.put("u", new Utf8("wibble"))
    Decoder[CPWrapper].decode(record, schema, DefaultFieldMapper) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG]("wibble"))
  }

  test("coproducts with case classes") {
    val schema = AvroSchema[CPWrapper]
    val gimble = new GenericData.Record(AvroSchema[Gimble])
    gimble.put("x", new Utf8("foo"))
    val record = new GenericData.Record(schema)
    record.put("u", gimble)
    Decoder[CPWrapper].decode(record, schema, DefaultFieldMapper) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo")))
  }

  test("coproducts with options") {
    val schema = AvroSchema[CPWithOption]
    val gimble = new GenericData.Record(AvroSchema[Gimble])
    gimble.put("x", new Utf8("foo"))
    val record = new GenericData.Record(schema)
    record.put("u", gimble)
    Decoder[CPWithOption].decode(record, schema, DefaultFieldMapper) shouldBe CPWithOption(Some(Coproduct[CPWrapper.ISBG](Gimble("foo"))))
  }

  test("coproducts") {
    val schema = AvroSchema[Coproducts]
    val record = new GenericData.Record(schema)
    record.put("union", new Utf8("foo"))
    val coproduct = Coproduct[Int :+: String :+: Boolean :+: CNil]("foo")
    Decoder[Coproducts].decode(record, schema, DefaultFieldMapper) shouldBe Coproducts(coproduct)
  }

  test("coproducts of coproducts") {
    val schema = AvroSchema[CoproductsOfCoproducts]
    val record = new GenericData.Record(schema)
    record.put("union", new Utf8("foo"))
    val coproduct = Coproduct[(Int :+: String :+: CNil) :+: Boolean :+: CNil](Coproduct[Int :+: String :+: CNil]("foo"))
    Decoder[CoproductsOfCoproducts].decode(record, schema, DefaultFieldMapper) shouldBe CoproductsOfCoproducts(coproduct)
  }
  
  test("coproducts with enum") {
    type OnlyColour = Colours.Value :+: CNil
    val schema = AvroSchema[CoproductOfEnum]
    
    val recordEnum = new GenericData.Record(schema)
    recordEnum.put("union", new EnumSymbol(schema.getField("union").schema(), "Red"))
    Decoder[CoproductOfEnum].decode(recordEnum, schema, DefaultFieldMapper) shouldBe CoproductOfEnum(Coproduct[OnlyColour](Colours.Red))
  }
  
  test("coproducts with enum or int") {
    type ColourOrInt = Colours.Value :+: Int :+: CNil
    val schema = AvroSchema[CoproductOfEnumOrInt]

    val recordInt = new GenericData.Record(schema)
    recordInt.put("union", 3)
    Decoder[CoproductOfEnumOrInt].decode(recordInt, schema, DefaultFieldMapper) shouldBe CoproductOfEnumOrInt(Coproduct[ColourOrInt](3))
    
    val recordEnum  = new GenericData.Record(schema)
    recordEnum.put("union", new EnumSymbol(schema.getField("union").schema(), "Red"))
    Decoder[CoproductOfEnumOrInt].decode(recordEnum, schema, DefaultFieldMapper) shouldBe CoproductOfEnumOrInt(Coproduct[ColourOrInt](Colours.Red))
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