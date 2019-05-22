package com.sksamuel.avro4s.streams.input

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, Decoder, DefaultNamingStrategy, Encoder, SchemaFor}
import org.scalatest.{FunSuite, Matchers}

trait InputStreamTest extends FunSuite with Matchers {

  def readData[T: SchemaFor : Decoder](out: ByteArrayOutputStream): T = readData(out.toByteArray)
  def readData[T: SchemaFor : Decoder](bytes: Array[Byte]): T = {
    AvroInputStream.data.from(bytes).build(implicitly[SchemaFor[T]].schema(DefaultNamingStrategy)).iterator.next()
  }

  def writeData[T: Encoder : SchemaFor](t: T): ByteArrayOutputStream = {
    val schema = AvroSchema[T]
    val out = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[T].to(out).build(schema)
    avro.write(t)
    avro.close()
    out
  }

  def readBinary[T: SchemaFor : Decoder](out: ByteArrayOutputStream): T = readBinary(out.toByteArray)
  def readBinary[T: SchemaFor : Decoder](bytes: Array[Byte]): T = {
    AvroInputStream.binary.from(bytes).build(implicitly[SchemaFor[T]].schema(DefaultNamingStrategy)).iterator.next()
  }

  def writeBinary[T: Encoder : SchemaFor](t: T): ByteArrayOutputStream = {
    val schema = AvroSchema[T]
    val out = new ByteArrayOutputStream
    val avro = AvroOutputStream.binary[T].to(out).build(schema)
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
