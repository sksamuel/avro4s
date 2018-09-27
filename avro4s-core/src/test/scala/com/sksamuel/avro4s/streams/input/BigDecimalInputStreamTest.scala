package com.sksamuel.avro4s.streams.input

class BigDecimalInputStreamTest extends InputStreamTest {

  case class BigDecimalTest(z: BigDecimal)
  case class BigDecimalOptionTest(z: Option[BigDecimal])
  case class BigDecimalSeqs(z: Seq[BigDecimal])
  case class BigDecimalDefault(z: BigDecimal = BigDecimal(1234.56))

  test("read write big decimal") {
    writeRead(BigDecimalTest(4.12))
  }

  test("read write big decimal with default value") {
    writeRead(BigDecimalDefault(), BigDecimalDefault(1234.56))
  }

  test("read write seq of big decimal") {
    writeRead(BigDecimalSeqs(Seq(56.3, 179.2)))
  }

  test("read write option of big decimal") {
    writeRead(BigDecimalOptionTest(Some(56.3)))
    writeRead(BigDecimalOptionTest(None))
  }
}