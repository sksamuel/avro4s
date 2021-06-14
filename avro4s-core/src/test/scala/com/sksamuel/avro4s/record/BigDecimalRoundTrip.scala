package com.sksamuel.avro4s.record

import com.sksamuel.avro4s.record.decoder.WithBigDecimal
import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder, ScalePrecision, SchemaFor}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BigDecimalRoundTrip extends AnyFunSuite with Matchers {

  test("BigDecimal round trip") {

    case class WithBigDecimal(decimal: BigDecimal)

    given ScalePrecision = ScalePrecision(3, 9)
    val schema = AvroSchema[WithBigDecimal]

    val encoded1 = Encoder[WithBigDecimal].encode(schema).apply(WithBigDecimal(BigDecimal(123.45)))
    val result1 = Decoder[WithBigDecimal].decode(schema).apply(encoded1)
    result1.decimal shouldBe BigDecimal(123.450) // will be padded to 3 dp

    val encoded2 = Encoder[WithBigDecimal].encode(schema).apply(WithBigDecimal(BigDecimal(123.4567)))
    val result2 = Decoder[WithBigDecimal].decode(schema).apply(encoded2)
    result2.decimal shouldBe BigDecimal(123.457) // will be rounded up

    val encoded3 = Encoder[WithBigDecimal].encode(schema).apply(WithBigDecimal(BigDecimal(123.456)))
    val result3 = Decoder[WithBigDecimal].decode(schema).apply(encoded3)
    result3.decimal shouldBe BigDecimal(123.456) // uses exactly 3 decimals
  }

}
