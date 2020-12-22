package com.sksamuel.avro4s.streams.output

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayOutputStream

class WrongSchema extends AnyFunSuite{
  test("Wrong schema at deserialization") {
    val baos = new ByteArrayOutputStream()
    val os = AvroOutputStream.binary[Test].to(baos).build()

    os.write(Test(3, None))
    os.flush()
    os.close


    val testInside = com.sksamuel.avro4s.AvroSchema[TestInside].toString
    val avroTestSchema = new org.apache.avro.Schema.Parser().parse(testInside)
    val schemaFor = com.sksamuel.avro4s.SchemaFor[Test](avroTestSchema)
    val schema = com.sksamuel.avro4s.AvroSchema[Test](schemaFor)

    val outputStream = AvroInputStream.binary[Test].from(baos.toByteArray).build(schema)
    println("---")
    println(outputStream.tryIterator.toList)

  }

  final case class TestInside(b: String)
  final case class Test(a: Int, inside: Option[TestInside])
}
