package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, FieldMapper, ScalePrecision, SchemaFor}
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}

case class BigDecimalSeqOption(biggies: Seq[Option[BigDecimal]])
case class BigDecimalSeq(biggies: Seq[BigDecimal])
case class BigDecimalDefault(decimal: BigDecimal = 964.55)

class BigDecimalSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "accept big decimal as logical type on bytes" in {
      case class Test(decimal: BigDecimal)
      val schema = AvroSchema[Test]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal.json"))
      schema shouldBe expected
    }
    "accept big decimal as logical type on bytes with custom scale and precision" in {
      implicit val sp = ScalePrecision(8, 20)
      case class Test(decimal: BigDecimal)
      val schema = AvroSchema[Test]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal-scale-and-precision.json"))
      schema shouldBe expected
    }
    "support big decimal with default" in {
      val schema = AvroSchema[BigDecimalDefault]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_default.json"))
      schema shouldBe expected
    }
    "suport Option[BigDecimal] as a union" in {
      case class BigDecimalOption(decimal: Option[BigDecimal])
      val schema = AvroSchema[BigDecimalOption]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_option.json"))
      schema shouldBe expected
    }
    "Seq[BigDecimal] be represented as an array of logical types" in {
      val schema = AvroSchema[BigDecimalSeq]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_seq.json"))
      schema shouldBe expected
    }
    "Seq[Option[BigDecimal]] be represented as an array of unions of nulls/bigdecimals" in {
      val schema = AvroSchema[BigDecimalSeqOption]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_seq_option.json"))
      schema shouldBe expected
    }
    "allow big decimals to be encoded as strings when custom typeclasses are provided" in {

      implicit val bigDecimalSchemaFor = com.sksamuel.avro4s.BigDecimals.AsString

      case class BigDecimalAsStringTest(decimal: BigDecimal)
      val schema = AvroSchema[BigDecimalAsStringTest]
      val expected = new org.apache.avro.Schema.Parser().parse(this.getClass.getResourceAsStream("/bigdecimal_as_string.json"))
      schema shouldBe expected
    }
    "allow big decimals to be encoded as FIXED when custom typeclasses are provided" in {

      implicit object BigDecimalAsFixedSchemaFor extends SchemaFor[BigDecimal] {
        override def schema(fieldMapper: FieldMapper): Schema = Schema.createFixed("bigdecimal", null, null, 55)
      }

      case class BigDecimalAsFixedTest(decimal: BigDecimal)
      val schema = AvroSchema[BigDecimalAsFixedTest]
      val expected = new org.apache.avro.Schema.Parser().parse(this.getClass.getResourceAsStream("/bigdecimal_as_fixed.json"))
      schema shouldBe expected
    }
    //
    //    "fail when trying to convert a BigDecimal into ByteBuffer without specifying the scale and precision and rounding mode and rounding is required" in {
    //      val n = BigDecimal(7.851)
    //      the[java.lang.ArithmeticException] thrownBy {
    //        BigDecimalFromValue.apply(BigDecimalToValue.apply(n))
    //      } should have message "Rounding necessary"
    //    }
    //
    //    "convert a BigDecimal into ByteBuffer with specifying the scale and precision and rounding mode and rounding is not required" in {
    //      val sp = ScaleAndPrecisionAndRoundingMode(3, 8, HALF_EVEN)
    //      val n = BigDecimal(7.85)
    //      BigDecimalFromValue(sp)(BigDecimalToValue(sp)(n)) shouldBe BigDecimal(7.850)
    //    }
    //
    //    "convert a BigDecimal into ByteBuffer with specifying the scale and precision and rounding mode and rounding is required" in {
    //      val sp = ScaleAndPrecisionAndRoundingMode(3, 8, HALF_EVEN)
    //      val n = BigDecimal(7.8516)
    //      BigDecimalFromValue(sp)(BigDecimalToValue(sp)(n)) shouldBe BigDecimal(7.852)
    //    }
  }
}