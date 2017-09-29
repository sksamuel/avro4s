package com.sksamuel.avro4s.macros

import com.sksamuel.avro4s.FromValue.BigDecimalFromValue
import com.sksamuel.avro4s.ScaleAndPrecisionAndRoundingMode
import com.sksamuel.avro4s.ToValue.BigDecimalToValue
import org.scalatest.{Matchers, WordSpec}

import scala.math.BigDecimal.RoundingMode.HALF_EVEN

class BigDecimalToValueTest extends WordSpec with Matchers {

  "BigDecimalToValue" should {

    "convert a BigDecimal into ByteBuffer without specifying the scale and precision and rounding mode and rounding is not required" in {
      val n = BigDecimal(7.8)
      BigDecimalFromValue.apply(BigDecimalToValue.apply(n)) shouldBe BigDecimal(7.80)
    }

    "fail when trying to convert a BigDecimal into ByteBuffer without specifying the scale and precision and rounding mode and rounding is required" in {
      val n = BigDecimal(7.851)
      the[java.lang.ArithmeticException] thrownBy {
        BigDecimalFromValue.apply(BigDecimalToValue.apply(n))
      } should have message "Rounding necessary"
    }

    "convert a BigDecimal into ByteBuffer with specifying the scale and precision and rounding mode and rounding is not required" in {
      val sp = ScaleAndPrecisionAndRoundingMode(3, 8, HALF_EVEN)
      val n = BigDecimal(7.85)
      BigDecimalFromValue(sp)(BigDecimalToValue(sp)(n)) shouldBe BigDecimal(7.850)
    }

    "convert a BigDecimal into ByteBuffer with specifying the scale and precision and rounding mode and rounding is required" in {
      val sp = ScaleAndPrecisionAndRoundingMode(3, 8, HALF_EVEN)
      val n = BigDecimal(7.8516)
      BigDecimalFromValue(sp)(BigDecimalToValue(sp)(n)) shouldBe BigDecimal(7.852)
    }
  }
}
