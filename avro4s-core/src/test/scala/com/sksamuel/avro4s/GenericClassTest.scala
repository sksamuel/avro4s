package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.scalatest.FunSuite

import scala.io.Source

class GenericClassTest extends FunSuite {
  test("Generic classes with different concrete types generate different type names with default streams") {
    val data = MyData(MyWrapper(1), Some(MyWrapper("")))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = AvroOutputStream.data[MyData](byteArrayOutputStream)
    outputStream.write(data)
    outputStream.close()
    byteArrayOutputStream.close()

    val inputStream = AvroInputStream.data[MyData](byteArrayOutputStream.toByteArray)
    assert(inputStream.iterator.next() === data)
    assert(inputStream.datumReader.getSchema === expectedSchema)
    assert(outputStream.schema === expectedSchema)
  }

  test("Generic classes with different concrete types generate different type names with binary streams") {
    val data = MyData(MyWrapper(1), Some(MyWrapper("")))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = AvroOutputStream.binary[MyData](byteArrayOutputStream)
    outputStream.write(data)
    outputStream.close()
    byteArrayOutputStream.close()

    val inputStream = AvroInputStream.binary[MyData](byteArrayOutputStream.toByteArray)
    assert(inputStream.iterator.next() === data)
  }

  val expectedSchema: Schema =
    new Schema.Parser().parse(
      Source.fromFile(getClass.getClassLoader.getResource("genericSchema.avsc").getFile).getLines().next()
    )

}
