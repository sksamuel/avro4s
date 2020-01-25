package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.math.BigDecimal.RoundingMode

class BigDecimalEncoderTest extends AnyFunSuite with Matchers {

  import scala.collection.JavaConverters._

  test("use byte array for decimal") {

    case class Test(decimal: BigDecimal)

    val schema = AvroSchemaV2[Test]

    val obj = Test(12.34)
    val s = schema.getField("decimal").schema()
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(12.34).bigDecimal, s, s.getLogicalType)

    EncoderV2[Test].encode(obj) shouldBe ImmutableRecord(schema, Vector(bytes))
  }

  test("allow decimals to be encoded as strings") {

    implicit val bigDecimalSchemaFor = SchemaForV2[BigDecimal](SchemaBuilder.builder.stringType)
    implicit val bigDecimalEncoder = EncoderV2[BigDecimal].withSchema(bigDecimalSchemaFor)

    case class Test(decimal: BigDecimal)

    val schema = AvroSchemaV2[Test]
    val record = EncoderV2[Test].encode(Test(123.456))
    record shouldBe ImmutableRecord(schema, Vector(new Utf8("123.456")))
  }

  test("Allow Override of roundingMode") {

    case class Test(decimal: BigDecimal)

    implicit val sp = ScalePrecision(2, 10)
    val schema = AvroSchemaV2[Test]
    val s = schema.getField("decimal").schema()

    implicit val roundingMode = RoundingMode.HALF_UP

    val bytesRoundedDown =
      new Conversions.DecimalConversion().toBytes(BigDecimal(12.34).bigDecimal, s, s.getLogicalType)
    EncoderV2[Test].encode(Test(12.3449)) shouldBe ImmutableRecord(schema, Vector(bytesRoundedDown))

    val bytesRoundedUp = new Conversions.DecimalConversion().toBytes(BigDecimal(12.35).bigDecimal, s, s.getLogicalType)
    EncoderV2[Test].encode(Test(12.345)) shouldBe ImmutableRecord(schema, Vector(bytesRoundedUp))
  }

  test("support optional big decimals") {

    case class Test(big: Option[BigDecimal])
    val schema = AvroSchemaV2[Test]

    val s = schema.getField("big").schema().getTypes.asScala.find(_.getType != Schema.Type.NULL).get
    val bytes =
      new Conversions.DecimalConversion().toBytes(BigDecimal(123.4).bigDecimal.setScale(2), s, s.getLogicalType)

    EncoderV2[Test].encode(Test(Some(123.4))) shouldBe ImmutableRecord(schema, Vector(bytes))
    EncoderV2[Test].encode(Test(None)) shouldBe ImmutableRecord(schema, Vector(null))
  }

  test("allow custom typeclass overrides") {

    implicit val bigDecimalAsString = SchemaForV2[BigDecimal](SchemaBuilder.builder.stringType)
    implicit val bigDecimalEncoder = EncoderV2[BigDecimal].withSchema(bigDecimalAsString)

    case class Test(decimal: BigDecimal)

    val schema = AvroSchemaV2[Test]
    EncoderV2[Test].encode(Test(123.66)) shouldBe ImmutableRecord(schema, Vector(new Utf8("123.66")))
  }

  test("allow bigdecimals to be encoded as generic fixed") {
    case class Test(s: BigDecimal)
    implicit val bigDecimalAsFixed = SchemaForV2[BigDecimal](
      LogicalTypes.decimal(10, 8).addToSchema(SchemaBuilder.fixed("BigDecimal").size(8)))
    implicit val bigDecimalEncoder = EncoderV2[BigDecimal].withSchema(bigDecimalAsFixed)
    val record = EncoderV2[Test].encode(Test(12345678)).asInstanceOf[GenericRecord]
    record.get("s").asInstanceOf[GenericData.Fixed].bytes().toList shouldBe Seq(0, 4, 98, -43, 55, 43, -114, 0)
  }
}
