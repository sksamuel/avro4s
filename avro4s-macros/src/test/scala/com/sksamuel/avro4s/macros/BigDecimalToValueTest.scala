package com.sksamuel.avro4s.macros

import com.sksamuel.avro4s.FromValue.BigDecimalFromValue
import com.sksamuel.avro4s.ScaleAndPrecision
import com.sksamuel.avro4s.ToValue.BigDecimalToValue
import org.scalatest.{Matchers, WordSpec}

class BigDecimalToValueTest extends WordSpec with Matchers {

  "BigDecimalToValue" should {
    "convert a BigDecimal into ByteBuffer (and back) with the default scale" in {
      val n = BigDecimal(5.00)
      BigDecimalFromValue.apply(BigDecimalToValue.apply(n)) shouldBe n
    }

    "convert a BigDecimal into ByteBuffer (and back) defining scale and precision" in {
      implicit val sp: ScaleAndPrecision = ScaleAndPrecision(3, 8)
      val n = BigDecimal(7.851).setScale(3)
      BigDecimalFromValue(sp)(BigDecimalToValue(sp)(n)) shouldBe n
    }

    "fail when trying to convert a BigDecimal into ByteBuffer without specifying the scale and precision" in {
      val n = BigDecimal(7.851)
      the[java.lang.ArithmeticException] thrownBy {
        BigDecimalFromValue.apply(BigDecimalToValue.apply(n))
      } should have message "Rounding necessary"
    }
  }
}
