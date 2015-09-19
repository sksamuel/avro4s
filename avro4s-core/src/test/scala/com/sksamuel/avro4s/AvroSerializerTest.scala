package com.sksamuel.avro4s

import java.nio.file.Files

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericData, GenericRecord, GenericDatumReader}
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class AvroSerializerTest extends WordSpec with Matchers {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence", Seq("painter", "architect"))

  "AvroSerializer" should {
    "write out simple records" in {

      val painters = Seq(michelangelo, raphael)
      val file = Files.createTempFile("avro", "avro")
      file.toFile.deleteOnExit()

      import AvroImplicits._

      val w = writerFor[Artist]
      val s = schemaFor[Artist]

      val writer = AvroOutputStream[Artist](file)
      writer.write(painters)

      val datumReader = new GenericDatumReader[GenericRecord](s.schema)
      val dataFileReader = new DataFileReader[GenericRecord](file.toFile, datumReader)
      println("Deserialized data is :")
      while (dataFileReader.hasNext) {
        val rec = dataFileReader.next(new GenericData.Record(s.schema))
        println(rec)
      }

      println(Source.fromFile(file.toFile).getLines().mkString("\n"))
    }
  }
}

case class Artist(name: String, yearOfBirth: Int, yearOfDeath: Int, birthplace: String, styles: Seq[String])