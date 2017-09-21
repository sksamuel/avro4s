package com.sksamuel.avro4s.macros

import com.sksamuel.avro4s.FromValue.BigDecimalFromValue
import com.sksamuel.avro4s.ScaleAndPrecisionAndRoundingMode
import com.sksamuel.avro4s.ToValue.BigDecimalToValue
import org.scalatest.{Matchers, WordSpec}

import scala.math.BigDecimal.RoundingMode.HALF_EVEN

class BigDecimalToValueTest extends WordSpec with Matchers {

  "BigDecimalToValue" should {
    "convert a BigDecimal into ByteBuffer (and back) with the default scale" in {
      val n = BigDecimal(5.00)
      BigDecimalFromValue.apply(BigDecimalToValue.apply(n)) shouldBe n
    }

    "convert a BigDecimal into ByteBuffer (and back) defining scale and precision and rounding mode" in {
      implicit val sp: ScaleAndPrecisionAndRoundingMode = ScaleAndPrecisionAndRoundingMode(3, 8, HALF_EVEN)
      val n = BigDecimal(7.8516)
      BigDecimalFromValue(sp)(BigDecimalToValue(sp)(n)) shouldBe BigDecimal(7.852)
    }

    "convert a BigDecimal into ByteBuffer without explicitly specifying the scale and precision and rounding mode" in {
      val n = BigDecimal(7.851)
      BigDecimalFromValue.apply(BigDecimalToValue.apply(n)) shouldBe BigDecimal(7.85)
    }
  }
}
