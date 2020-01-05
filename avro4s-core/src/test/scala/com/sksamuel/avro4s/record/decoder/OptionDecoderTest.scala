package com.sksamuel.avro4s.record.decoder

import java.util

import com.sksamuel.avro4s.{AvroSchema, Decoder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class OptionBoolean(b: Option[Boolean])
case class OptionString(s: Option[String])
case class RequiredString(s: String)


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
      Decoder[OptionString].decode(record1) shouldBe OptionString(Some("hello"))

      val record2 = new GenericData.Record(schema)
      record2.put("s", null)
      Decoder[OptionString].decode(record2) shouldBe OptionString(None)
    }
    "support decoding required fields as Option" in {
      val requiredStringSchema = AvroSchema[RequiredString]

      val requiredStringRecord = new GenericData.Record(requiredStringSchema)
      requiredStringRecord.put("s", "hello")
      Decoder[OptionString].decode(requiredStringRecord) shouldBe OptionString(Some("hello"))
    }
    "support boolean options" in {
      val schema = AvroSchema[OptionBoolean]

      val record1 = new GenericData.Record(schema)
      record1.put("b", true)
      Decoder[OptionBoolean].decode(record1) shouldBe OptionBoolean(Some(true))

      val record2 = new GenericData.Record(schema)
      record2.put("b", null)
      Decoder[OptionBoolean].decode(record2) shouldBe OptionBoolean(None)
    }
    "if a field is missing, use default value" in {
      val record1 = new GenericData.Record(AvroSchema[SchemaWithoutExpectedField])

      // TODO clarify is it really desired to decode records having a different schema?
      Decoder[OptionStringDefault].decode(record1) shouldBe OptionStringDefault(Some("cupcat"))
    }
    "if an enum field is missing, use default value" in {
      val record1 = new GenericData.Record(AvroSchema[SchemaWithoutExpectedField])

      // TODO clarify is it really desired to decode records having a different schema?
      Decoder[OptionEnumDefault].decode(record1) shouldBe OptionEnumDefault(Some(CuppersOptionEnum))
    }
    "decode a null field to None" in {
      val schema = AvroSchema[OptionEnumDefaultWithNone]

      val record1 = new GenericData.Record(schema)
      record1.put("s", null)
      record1.put("t", "cupcat")

      Decoder[OptionEnumDefaultWithNone].decode(record1) shouldBe OptionEnumDefaultWithNone(None, "cupcat")
    }
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

      Decoder[OptionOfSeqOfCaseClass].decode(record1) shouldBe OptionOfSeqOfCaseClass(Some(List(Foo(true), Foo(false))))

      val record2 = new GenericData.Record(schema)
      record2.put("foo", null)

      Decoder[OptionOfSeqOfCaseClass].decode(record2) shouldBe OptionOfSeqOfCaseClass(None)
    }

  }
}

