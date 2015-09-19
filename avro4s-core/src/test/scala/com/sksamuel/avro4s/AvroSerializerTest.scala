package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableInput}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class AvroSerializerTest extends WordSpec with Matchers {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese")
  //, Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence") //, Seq("painter", "architect"))

  "AvroSerializer" should {
    "write out simple records" in {

      val painters = Seq(michelangelo, raphael)
      val temp = Files.createTempFile("avro", "avro")
      temp.toFile.deleteOnExit()

      import AvroImplicits._

      val w = writerFor[Artist]

      val writer = AvroOutputStream[Artist](temp)
      writer.write(painters)
      println(Source.fromFile(temp.toFile).getLines().mkString("\n"))
    }
  }
}

class AvroOutputStream[T](os: OutputStream)(implicit s: AvroSchema[T], w: AvroSerializer[T]) {

  val datumWriter = new GenericDatumWriter[GenericRecord](s.schema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(s.schema, os)

  def write(ts: Seq[T]): Unit = {
    ts.foreach(t => {
      val record = w.write(t)
      dataFileWriter.append(record)
    })
  }

  def close(): Unit = dataFileWriter.close()
}

object AvroOutputStream {
  def apply[T](file: File)(implicit s: AvroSchema[T], w: AvroSerializer[T]): AvroOutputStream[T] = apply(file.toPath)
  def apply[T](path: Path)(implicit s: AvroSchema[T], w: AvroSerializer[T]): AvroOutputStream[T] = {
    new AvroOutputStream[T](Files.newOutputStream(path))
  }
}

class AvroInputStream[T](in: SeekableInput)(implicit writer: AvroSchema[T]) {

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

case class Artist(name: String, yearOfBirth: Int, yearOfDeath: Int, birthplace: String)

//, styles: Seq[String])