package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}

class DecimalSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "accept big decimal as logical type on bytes" in {
      case class Test(decimal: BigDecimal)
      val schema = SchemaEncoder[Test].encode
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal.avsc"))
      schema shouldBe expected
    }
    "support big decimal with default" in {
      case class BigDecimalDefault(decimal: BigDecimal = 964.55)
      val schema = SchemaEncoder[BigDecimalDefault].encode
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal_default.json"))
      schema shouldBe expected
    }
  }
}
