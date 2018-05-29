package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.apache.avro.{Schema, SchemaParseException}
import org.scalatest.FunSuite

import scala.io.Source

class GenericClassTest extends FunSuite {
  test("Generic classes with different concrete types and AvroSpecificGeneric enabled " +
    "generate different type names with default streams") {
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

  test("Generic classes with different concrete types and AvroSpecificGeneric explicitly disabled " +
    "should throw SchemaParseException because of duplicated types") {
    val data = DisabledMyData(DisabledWrapper(1), Some(DisabledWrapper("")))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    intercept[org.apache.avro.SchemaParseException] {
      AvroOutputStream.data[DisabledMyData](byteArrayOutputStream)
    }
  }

  test("Generic classes with different concrete types and AvroSpecificGeneric not annotated" +
    "should throw SchemaParseException because of duplicated types") {
    val data = NotAnnotatedMyData(NotAnnotatedWrapper(1), Some(NotAnnotatedWrapper("")))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    intercept[org.apache.avro.SchemaParseException] {
      AvroOutputStream.data[DisabledMyData](byteArrayOutputStream)
    }
  }

  test("Generic classes with different concrete types and AvroSpecificGeneric annotation generate " +
    "different type names with binary streams") {
    val data = MyData(MyWrapper(1), Some(MyWrapper("")))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = AvroOutputStream.binary[MyData](byteArrayOutputStream)
    outputStream.write(data)
    outputStream.close()
    byteArrayOutputStream.close()

    val inputStream = AvroInputStream.binary[MyData](byteArrayOutputStream.toByteArray)
    assert(inputStream.iterator.next() === data)
  }

  test("SchemaFor for generic classes with different concrete types and AvroSpecificGeneric annotation " +
    "should generate the specific schema") {
    val schema = implicitly[SchemaFor[MyData]].apply()
    assert(schema === expectedSchema)
  }

  test("SchemaFor for generic classes with different concrete types and AvroSpecificGeneric explicitly " +
    "disabled should throw SchemaParseException") {
    intercept[SchemaParseException] {
      implicitly[SchemaFor[DisabledMyData]].apply().toString
    }
  }

  test("SchemaFor for generic classes with different concrete types not annotated with AvroSpecificGeneric " +
    "should throw SchemaParseException") {
    intercept[SchemaParseException] {
      implicitly[SchemaFor[NotAnnotatedMyData]].apply().toString
    }
  }

  val expectedSchema: Schema =
    new Schema.Parser().parse(
      Source.fromFile(getClass.getClassLoader.getResource("genericSchema.avsc").getFile).getLines().next()
    )

}
