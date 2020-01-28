package com.sksamuel.avro4s.streams.input

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait InputStreamTest extends AnyFunSuite with Matchers {

  def readData[T: SchemaForV2: Decoder](out: ByteArrayOutputStream): T = readData(out.toByteArray)
  def readData[T: SchemaForV2: Decoder](bytes: Array[Byte]): T = {
    AvroInputStream.data.from(bytes).build(implicitly[SchemaForV2[T]].schema).iterator.next()
  }

  def writeData[T: EncoderV2: SchemaForV2](t: T): ByteArrayOutputStream = {
    val out = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[T].to(out).build()
    avro.write(t)
    avro.close()
    out
  }

  def readBinary[T: SchemaForV2: Decoder](out: ByteArrayOutputStream): T = readBinary(out.toByteArray)
  def readBinary[T: SchemaForV2: Decoder](bytes: Array[Byte]): T = {
    AvroInputStream.binary.from(bytes).build(implicitly[SchemaForV2[T]].schema).iterator.next()
  }

  def writeBinary[T: EncoderV2](t: T): ByteArrayOutputStream = {
    val out = new ByteArrayOutputStream
    val avro = AvroOutputStream.binary[T].to(out).build()
    avro.write(t)
    avro.close()
    out
  }

  def writeRead[T: EncoderV2: Decoder: SchemaForV2](t: T): Unit = {
    {
      val out = writeData(t)
      readData(out) shouldBe t
    }
    {
      val out = writeBinary(t)
      readBinary(out) shouldBe t
    }
  }

  def writeRead[T: EncoderV2: Decoder: SchemaForV2](t: T, expected: T): Unit = {
    {
      val out = writeData(t)
      readData(out) shouldBe expected
    }
    {
      val out = writeBinary(t)
      readBinary(out) shouldBe expected
    }
  }
}
