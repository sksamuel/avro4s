package com.sksamuel.avro4s

import java.nio.file.Files

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroSerializerTest extends WordSpec with Matchers with Timeouts {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence", Seq("painter", "architect"))

  "AvroSerializer" should {
    "write out simple records" in {

      val painters = Seq(michelangelo, raphael)
      val path = Files.createTempFile("AvroSerializerTest", ".avro")
      path.toFile.deleteOnExit()

      import AvroImplicits._

      import scala.collection.JavaConverters._

      val w = writerFor[Artist]
      val s = schemaFor[Artist]

      val writer = AvroOutputStream[Artist](path)
      writer.write(painters)
      writer.close()

      val datumReader = new GenericDatumReader[GenericRecord](s.schema)
      val dataFileReader = new DataFileReader[GenericRecord](path.toFile, datumReader)

      dataFileReader.hasNext
      val rec1 = dataFileReader.next(new GenericData.Record(s.schema))
      rec1.get("name").toString shouldBe michelangelo.name
      rec1.get("yearOfBirth").toString.toInt shouldBe michelangelo.yearOfBirth
      rec1.get("yearOfDeath").toString.toInt shouldBe michelangelo.yearOfDeath
      val styles1 = rec1.get("styles").asInstanceOf[GenericData.Array[Utf8]].asScala.toSet
      styles1.map(_.toString) shouldBe michelangelo.styles.toSet

      dataFileReader.hasNext
      val rec2 = dataFileReader.next(new GenericData.Record(s.schema))
      rec2.get("name").toString shouldBe raphael.name
      rec2.get("yearOfBirth").toString.toInt shouldBe raphael.yearOfBirth
      rec2.get("yearOfDeath").toString.toInt shouldBe raphael.yearOfDeath
      val styles2 = rec2.get("styles").asInstanceOf[GenericData.Array[Utf8]].asScala.toSet
      styles2.map(_.toString) shouldBe raphael.styles.toSet

    }
  }
}

