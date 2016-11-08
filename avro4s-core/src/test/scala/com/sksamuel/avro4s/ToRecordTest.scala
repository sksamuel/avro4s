package com.sksamuel.avro4s

import org.scalatest.{FlatSpec, Matchers}

case class WithBigDecimal(decimal: BigDecimal)

class ToRecordTest extends FlatSpec with Matchers {

  "ToRecord" should "use byte array for decimal" in {
    val obj = WithBigDecimal(12.34)
    val record = ToRecord[WithBigDecimal](obj)
    record.toString shouldBe """{"decimal": {"bytes": "12.34"}}"""
  }
}
