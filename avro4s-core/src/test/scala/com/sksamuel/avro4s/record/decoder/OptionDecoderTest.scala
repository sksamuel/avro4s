package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.Decoder
import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.ImmutableRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util

case class OptionBoolean(b: Option[Boolean])
case class OptionString(s: Option[String])
case class RequiredString(s: String)

case class OptionEither(o: Option[Either[String, Boolean]])

sealed trait Dimension
case class HeightDimension(height: Double) extends Dimension
case class WidthDimension(width: Double) extends Dimension
case class MeasurableThing(dimension: Option[Dimension])

case class OptionDibble(o: Option[Dibble])

sealed trait CupcatOptionEnum
case object CuppersOptionEnum extends CupcatOptionEnum
case object SnoutleyOptionEnum extends CupcatOptionEnum

case class OptionStringDefault(s: Option[String] = Option("cupcat"))
case class OptionEnumDefault(s: Option[CupcatOptionEnum] = Option(CuppersOptionEnum))

case class OptionEnumDefaultWithNone(s: Option[CupcatOptionEnum] = Option(CuppersOptionEnum), t: String)
case class OptionStringDefaultWithNone(s: Option[String] = Option("cupcat"), t: String)

case class SchemaWithoutExpectedField(t: String)

case class OptionOfSeqOfCaseClass(foo: Option[Seq[Foo]])

class OptionDecoderTest extends AnyWordSpec with Matchers {

  "Decoder" should {
    "support String options" in {
      val schema = AvroSchema[OptionString]

      val record1 = new GenericData.Record(schema)
      record1.put("s", "hello")
      Decoder[OptionString].decode(schema).apply(record1) shouldBe OptionString(Some("hello"))

      val record2 = new GenericData.Record(schema)
      record2.put("s", null)
      Decoder[OptionString].decode(schema).apply(record2) shouldBe OptionString(None)
    }
    "support boolean options" in {
      val schema = AvroSchema[OptionBoolean]

      val record1 = new GenericData.Record(schema)
      record1.put("b", true)
      Decoder[OptionBoolean].decode(schema).apply(record1) shouldBe OptionBoolean(Some(true))

      val record2 = new GenericData.Record(schema)
      record2.put("b", null)
      Decoder[OptionBoolean].decode(schema).apply(record2) shouldBe OptionBoolean(None)
    }
    // todo once magnolia has scala 3 default support
    // "if a field is missing, use default value" in {
    //   val schema = AvroSchema[SchemaWithoutExpectedField]
    //   val record = new GenericData.Record(AvroSchema[SchemaWithoutExpectedField])
    //   Decoder[OptionStringDefault].decode(schema).apply(record) shouldBe OptionStringDefault(Some("cupcat"))
    // }
    // todo once magnolia has scala 3 default support
    // "if an enum field is missing, use default value" in {
    //   val schema = AvroSchema[SchemaWithoutExpectedField]
    //   val record = new GenericData.Record(AvroSchema[SchemaWithoutExpectedField])
    //   Decoder[OptionEnumDefault].decode(schema).apply(record) shouldBe OptionEnumDefault(Some(CuppersOptionEnum))
    // }
    // todo once magnolia has scala 3 default support
    //    "decode a null field to None" in {
    //      val schema = AvroSchema[OptionEnumDefaultWithNone]
    //      val record = new GenericData.Record(schema)
    //      record.put("s", null)
    //      record.put("t", "cupcat")
    //      Decoder[OptionEnumDefaultWithNone].decode(schema).apply(record) shouldBe OptionEnumDefaultWithNone(None, "cupcat")
    //    }
    "option of seq of case class" in {
      val schema = AvroSchema[OptionOfSeqOfCaseClass]
      val unionSchema = schema.getField("foo").schema()
      require(unionSchema.getType == Schema.Type.UNION)
      val arraySchema = unionSchema.getTypes.get(1)
      require(arraySchema.getType == Schema.Type.ARRAY)
      val fooSchema = arraySchema.getElementType

      val foo1 = new GenericData.Record(fooSchema)
      foo1.put("b", true)

      val foo2 = new GenericData.Record(fooSchema)
      foo2.put("b", false)

      val array = new GenericData.Array(arraySchema, util.Arrays.asList(foo1, foo2))

      val record1 = new GenericData.Record(schema)
      record1.put("foo", array)

      Decoder[OptionOfSeqOfCaseClass].decode(schema).apply(record1) shouldBe OptionOfSeqOfCaseClass(Some(List(Foo(true), Foo(false))))

      val record2 = new GenericData.Record(schema)
      record2.put("foo", null)

      Decoder[OptionOfSeqOfCaseClass].decode(schema).apply(record2) shouldBe OptionOfSeqOfCaseClass(None)
    }

    "option of either" in {
      val schema = AvroSchema[OptionEither]
      val unionSchema = schema.getField("o").schema()
      require(unionSchema.getType == Schema.Type.UNION)
      val stringSchema = unionSchema.getTypes.get(1)
      require(stringSchema.getType == Schema.Type.STRING)
      val booleanSchema = unionSchema.getTypes.get(2)
      require(booleanSchema.getType == Schema.Type.BOOLEAN)

      val noneRecord = new GenericData.Record(schema)
      noneRecord.put("o", null)

      val leftRecord = new GenericData.Record(schema)
      leftRecord.put("o", new Utf8("foo"))

      val rightRecord = new GenericData.Record(schema)
      rightRecord.put("o", java.lang.Boolean.valueOf(true))

      Decoder[OptionEither].decode(schema).apply(noneRecord) shouldBe OptionEither(None)
      Decoder[OptionEither].decode(schema).apply(leftRecord) shouldBe OptionEither(Some(Left("foo")))
      Decoder[OptionEither].decode(schema).apply(rightRecord) shouldBe OptionEither(Some(Right(true)))
    }

    "option of sealed trait" in {
      val schema = AvroSchema[MeasurableThing]

      val heightRecord = new GenericData.Record(schema)
      val heightDimensionRecord = new GenericData.Record(AvroSchema[HeightDimension])
      heightDimensionRecord.put("height", 1.23)
      heightRecord.put("dimension", heightDimensionRecord)

      val weightRecord = new GenericData.Record(schema)
      val weightDimensionRecord = new GenericData.Record(AvroSchema[WidthDimension])
      weightDimensionRecord.put("width", 1.23)
      weightRecord.put("dimension", weightDimensionRecord)

      val noneRecord = new GenericData.Record(schema)
      noneRecord.put("dimension", null)

      Decoder[MeasurableThing].decode(schema)(heightRecord) shouldBe MeasurableThing(Some(HeightDimension(1.23)))
      Decoder[MeasurableThing].decode(schema)(weightRecord) shouldBe MeasurableThing(Some(WidthDimension(1.23)))
      Decoder[MeasurableThing].decode(schema)(noneRecord) shouldBe MeasurableThing(None)
    }

    "option of enum" in {
      val schema = AvroSchema[OptionDibble]
      val dibbleSchema = AvroSchema[Dibble]

      val dobbleRecord = new GenericData.Record(schema)
      dobbleRecord.put("o", new GenericData.EnumSymbol(dibbleSchema, Dobble))

      val dabbleRecord = new GenericData.Record(schema)
      dabbleRecord.put("o", new GenericData.EnumSymbol(dibbleSchema, Dabble))

      val noneRecord = new GenericData.Record(schema)
      noneRecord.put("o", null)

      Decoder[OptionDibble].decode(schema)(dobbleRecord) shouldBe OptionDibble(Some(Dobble))
      Decoder[OptionDibble].decode(schema)(dabbleRecord) shouldBe OptionDibble(Some(Dabble))
      Decoder[OptionDibble].decode(schema)(noneRecord) shouldBe OptionDibble(None)
    }

    "Option with non-None default" in {
      case class Foo(
        bar: Option[Int] = Some(42)
      )
      val schema = AvroSchema[Foo]
      for (foo <- Seq(Foo(), Foo(Some(17)), Foo(None))) {
        val encoded = Encoder[Foo].encode(schema).apply(foo)
        val decoded = Decoder[Foo].decode(schema).apply(encoded)
        decoded shouldBe foo
      }
    }
  }
}
