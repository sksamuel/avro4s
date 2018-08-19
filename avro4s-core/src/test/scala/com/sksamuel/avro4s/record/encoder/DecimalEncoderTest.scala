package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, SchemaEncoder}
import org.scalatest.{FlatSpec, Matchers}

case class WithBigDecimal(decimal: BigDecimal)

class DecimalEncoderTest extends FlatSpec with Matchers {

  "Encoder" should "use byte array for decimal" in {
    val obj = WithBigDecimal(12.34)
    val schema = SchemaEncoder[WithBigDecimal].encode()
    Encoder[WithBigDecimal].encode(obj, schema) shouldBe InternalRecord(schema, Vector(12.34))
  }
}
