package com.sksamuel.avro4s

import java.io.OutputStream
import java.nio.file.Files
import java.util.UUID

import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableInput}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.scalatest.{Matchers, WordSpec}

class AvroWriterTest extends WordSpec with Matchers {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence", Seq("painter", "architect"))

  "AcroWrit" should {
    "qwe" in {

      import SchemaGenerator._

      val painters = Seq(michelangelo, raphael)
      val temp = Files.createTempFile("avro", "avro")
      temp.toFile.deleteOnExit()

      val writer = new AvroWriter[Artist](Files.newOutputStream(temp))
      writer.write(painters)
    }
  }
}

class AvroWriter[T](os: OutputStream)(implicit writer: AvroSchemaWriter[T]) {

  val datumWriter = new GenericDatumWriter[GenericRecord](writer.schema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(writer.schema, os)

  def write(ts: Seq[T]): Unit = {
    val record = new Record(writer.schema)
    record.put("id", UUID.randomUUID.toString)
    record.put("student_id", UUID.randomUUID.toString)
    record.put("university_id", UUID.randomUUID.toString)
    dataFileWriter.append(record)
  }

  def close(): Unit = dataFileWriter.close()
}

class AvroReader[T](in: SeekableInput)(implicit writer: AvroSchemaWriter[T]) {

  val datumReader = new GenericDatumReader[GenericRecord](writer.schema)
  val dataFileReader = new DataFileReader[GenericRecord](in: SeekableInput, datumReader)

  def iterator: Iterator[T] = {
    println("Deserialized data is :")
    new Iterator[T] {
      override def hasNext: Boolean = dataFileReader.hasNext
      override def next(): T = dataFileReader.next(new GenericData.Record(writer.schema)).asInstanceOf[T]
    }
  }
}

case class Artist(name: String, yearOfBirth: Int, yearOfDeath: Int, birthplace: String, style: Seq[String])