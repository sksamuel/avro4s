package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}

case class BigDecimalSeqOption(biggies: Seq[Option[BigDecimal]])
case class BigDecimalSeq(biggies: Seq[BigDecimal])

class DecimalSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "accept big decimal as logical type on bytes" in {
      case class Test(decimal: BigDecimal)
      val schema = SchemaEncoder[Test].encode
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal.json"))
      schema shouldBe expected
    }
    "support big decimal with default" in {
      case class BigDecimalDefault(decimal: BigDecimal = 964.55)
      val schema = SchemaEncoder[BigDecimalDefault].encode
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_default.json"))
      schema shouldBe expected
    }
    "suport Option[BigDecimal] as a union" in {
      case class BigDecimalOption(decimal: Option[BigDecimal])
      val schema = SchemaFor[BigDecimalOption]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_option.json"))
      schema shouldBe expected
    }
    "support nested BigDecimal with default value" in {
      case class BigDecimalNestedDefault(decimal: BigDecimal = 964.55)
      val schema = SchemaFor[BigDecimalNestedDefault]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_nested_default.json"))
      schema shouldBe expected
    }
    "Seq[BigDecimal] be represented as an array of logical types" in {
      val schema = SchemaFor[BigDecimalSeq]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_seq.json"))
      schema shouldBe expected
    }
    "Seq[Option[BigDecimal]] be represented as an array of unions of nulls/bigdecimals" in {
      val schema = SchemaFor[BigDecimalSeqOption]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_seq_option.json"))
      schema shouldBe expected
    }
  }
}
