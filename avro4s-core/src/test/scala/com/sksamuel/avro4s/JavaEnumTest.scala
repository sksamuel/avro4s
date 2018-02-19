package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.scalatest.{FlatSpec, Matchers}

case class JavaEnumOptional(maybewine: Option[Wine])

class JavaEnumTest extends FlatSpec with Matchers {

  def read[T](out: ByteArrayOutputStream)(implicit schema: SchemaFor[T]): GenericRecord = read(out.toByteArray)
  def read[T](bytes: Array[Byte])(implicit schema: SchemaFor[T]): GenericRecord = {
    val datumReader = new GenericDatumReader[GenericRecord](schema())
    val dataFileReader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datumReader)
    new Iterator[GenericRecord] {
      override def hasNext: Boolean = dataFileReader.hasNext
      override def next(): GenericRecord = dataFileReader.next
    }.toList.head
  }

  "Schemas" should "accept java enums" in {
    case class Test(wine: Wine)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/enum.json"))
    val schema = SchemaFor[Test]()
    schema.toString(true) shouldBe expected.toString(true)
  }

  it should "support default options of enum values" in {
    val schema = SchemaFor[JavaEnumOptional]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_java_enum.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  "streams" should "support java enums" in {

    val data = WineCrate(Wine.Malbec)

    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[WineCrate](output)
    avro.write(data)
    avro.close()

    val in = AvroInputStream.data[WineCrate](output.toByteArray)
    in.iterator.toList shouldBe List(data)
    in.close()
  }

  it should "support optional java enums" in {
    val a = JavaEnumOptional(Some(Wine.Malbec))
    val b = JavaEnumOptional(None)
    val c = JavaEnumOptional(Some(Wine.Merlot))

    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[JavaEnumOptional](output)
    avro.write(List(a, b, c))
    avro.close()

    val in = AvroInputStream.data[JavaEnumOptional](output.toByteArray)
    in.iterator.toList shouldBe List(a, b, c)
    in.close()
  }
}
