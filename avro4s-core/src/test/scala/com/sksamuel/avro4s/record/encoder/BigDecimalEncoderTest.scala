package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.SchemaFor.StringSchemaFor
import com.sksamuel.avro4s.internal.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, Schema}
import org.scalatest.{FunSuite, Matchers}

class BigDecimalEncoderTest extends FunSuite with Matchers {

  import scala.collection.JavaConverters._

  test("use byte array for decimal") {

    case class Test(decimal: BigDecimal)

    val schema = AvroSchema[Test]

    val obj = Test(12.34)
    val s = schema.getField("decimal").schema()
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(12.34).bigDecimal, s, s.getLogicalType)

    Encoder[Test].encode(obj, schema) shouldBe ImmutableRecord(schema, Vector(bytes))
  }

  test("support optional big decimals") {

    case class Test(big: Option[BigDecimal])
    val schema = AvroSchema[Test]

    val s = schema.getField("big").schema().getTypes.asScala.find(_.getType != Schema.Type.NULL).get
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(123.4).bigDecimal.setScale(2), s, s.getLogicalType)

    Encoder[Test].encode(Test(Some(123.4)), schema) shouldBe ImmutableRecord(schema, Vector(bytes))
    Encoder[Test].encode(Test(None), schema) shouldBe ImmutableRecord(schema, Vector(null))
  }

  test("allow custom typeclass overrides") {

    implicit object BigDecimalAsString extends SchemaFor[BigDecimal] {
      override def schema: Schema = StringSchemaFor.schema
    }

    case class Test(decimal: BigDecimal)

    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(123.66), schema) shouldBe ImmutableRecord(schema, Vector(new Utf8("123.66")))
  }
}
