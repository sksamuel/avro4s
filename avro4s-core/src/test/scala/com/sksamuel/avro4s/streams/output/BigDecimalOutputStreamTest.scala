package com.sksamuel.avro4s.streams.output

import java.nio.ByteBuffer

import org.apache.avro.generic.GenericData

class BigDecimalOutputStreamTest extends OutputStreamTest {

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

  test("write seq of big decimal") {
    case class Test(z: Seq[BigDecimal])
    writeRead(Test(Seq(BigDecimal(563, 2), BigDecimal(1792, 2)))) { record =>
      val array = record.get("z").asInstanceOf[GenericData.Array[ByteBuffer]]
      val buffer1 = array.get(0)
      val bytes1 = Array.ofDim[Byte](buffer1.remaining())
      buffer1.get(bytes1)
      BigDecimal(BigInt(bytes1), 2) shouldBe BigDecimal(5.63)
      val buffer2 = array.get(1)
      val bytes2 = Array.ofDim[Byte](buffer2.remaining())
      buffer2.get(bytes2)
      BigDecimal(BigInt(bytes2), 2) shouldBe BigDecimal(17.92)
    }
  }

  test("write option of big decimal") {
    case class Test(z: Option[BigDecimal])
    writeRead(Test(Some(BigDecimal(563, 2)))) { record =>
      val buffer = record.get("z").asInstanceOf[ByteBuffer]
      val bytes = Array.ofDim[Byte](buffer.remaining())
      buffer.get(bytes)
      BigDecimal(BigInt(bytes), 2) shouldBe BigDecimal(5.63)
    }
    writeRead(Test(None)) { record =>
      val buffer = record.get("z")
      buffer shouldBe null
    }
  }
}