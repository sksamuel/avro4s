package com.sksamuel.avro4s.streams.output

import java.nio.ByteBuffer

class DecimalOutputStreamTest extends OutputStreamTest {

  test("write big decimal") {
    case class Test(z: BigDecimal)
    writeRead(Test(4.12)) { record =>
      val buffer = record.get("z").asInstanceOf[ByteBuffer]
      val bytes = Array.ofDim[Byte](buffer.remaining())
      buffer.get(bytes)
      BigDecimal(BigInt(bytes), 2) shouldBe BigDecimal(4.12)
    }
  }

  test("write big decimal with default value") {
    case class Test(z: BigDecimal = BigDecimal(1234.56))
    writeRead(Test()) { record =>
      val buffer = record.get("z").asInstanceOf[ByteBuffer]
      val bytes = Array.ofDim[Byte](buffer.remaining())
      buffer.get(bytes)
      BigDecimal(BigInt(bytes), 2) shouldBe BigDecimal(1234.56)
    }
  }
}