package com.sksamuel.avro4s.streams.input

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.internal.{AvroSchema, Decoder, Encoder, SchemaFor}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import org.scalatest.{FunSuite, Matchers}

trait InputStreamTest extends FunSuite with Matchers {

  def readData[T: SchemaFor : Decoder](out: ByteArrayOutputStream): T = readData(out.toByteArray)
  def readData[T: SchemaFor : Decoder](bytes: Array[Byte]): T = {
    AvroInputStream.data(bytes, implicitly[SchemaFor[T]].schema).iterator.next()
  }

  def writeData[T: Encoder : SchemaFor](t: T): ByteArrayOutputStream = {
    val schema = AvroSchema[T]
    val out = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[T](out, schema)
    avro.write(t)
    avro.close()
    out
  }

  def readBinary[T: SchemaFor : Decoder](out: ByteArrayOutputStream): T = readBinary(out.toByteArray)
  def readBinary[T: SchemaFor : Decoder](bytes: Array[Byte]): T = {
    AvroInputStream.binary(bytes, implicitly[SchemaFor[T]].schema).iterator.next()
  }

  def writeBinary[T: Encoder : SchemaFor](t: T): ByteArrayOutputStream = {
    val schema = AvroSchema[T]
    val out = new ByteArrayOutputStream
    val avro = AvroOutputStream.binary[T](out, schema)
    avro.write(t)
    avro.close()
    out
  }

  def writeRead[T: Encoder : Decoder : SchemaFor](t: T): Unit = {
    {
      val out = writeData(t)
      readData(out) shouldBe t
    }
    {
      val out = writeBinary(t)
      readBinary(out) shouldBe t
    }
  }

  def writeRead[T: Encoder : Decoder : SchemaFor](t: T, expected: T): Unit = {
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
