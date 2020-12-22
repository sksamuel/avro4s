package com.sksamuel.avro4s.streams.output

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, Encoder}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Work(name: String, year: Int, style: Style)
case class Composer(name: String, birthplace: String, works: Seq[Work])

class BinaryStreamsTest extends AnyWordSpec with Matchers {

  val ennio = Composer("ennio morricone", "rome", Seq(Work("legend of 1900", 1986, Style.Classical), Work("ecstasy of gold", 1969, Style.Classical)))
  val hans = Composer("hans zimmer", "frankfurt", Seq(Work("batman begins", 2007, Style.Modern), Work("dunkirk", 2017, Style.Modern)))

  "Avro binary streams" should {
    "not write schemas" in {

      implicit val schema = AvroSchema[Composer]
      implicit val encoder = Encoder[Composer]

      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Composer].to(baos).build()
      output.write(ennio)
      output.write(hans)
      output.close()

      // the schema should not be written in a binary stream
      new String(baos.toByteArray) should not include "birthplace"
      new String(baos.toByteArray) should not include "compositions"
      new String(baos.toByteArray) should not include "year"
      new String(baos.toByteArray) should not include "style"
    }
    "read and write" in {

      implicit val schema = AvroSchema[Composer]
      implicit val encoder = Encoder[Composer]

      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Composer].to(baos).build()
      output.write(ennio)
      output.write(hans)
      output.close()

      val in = AvroInputStream.binary[Composer].from(baos.toByteArray).build(schema)
      in.iterator.toList shouldBe List(ennio, hans)
      in.close()
    }

    "finish with error" in {
      val work = Work("work", 2020, Style.Classical)
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Work].to(baos).build()

      output.write(work)
      output.flush()
      output.close()


      val schemaString = AvroSchema[Composer].toString
      val avroTestSchema = new org.apache.avro.Schema.Parser().parse(schemaString)
      val schemaFor = com.sksamuel.avro4s.SchemaFor[Composer](avroTestSchema)
      val schema = com.sksamuel.avro4s.AvroSchema[Composer](schemaFor)


      val in = AvroInputStream.binary[Composer].from(baos.toByteArray).build(schema)
      //in.iterator.toList shouldBe List(ennio, hans)
      println(in.tryIterator.toList)
      //in.close()



//      val testInside = com.sksamuel.avro4s.AvroSchema[TestInside].toString
//      val avroTestSchema = new org.apache.avro.Schema.Parser().parse(testInside)
//      val schemaFor = com.sksamuel.avro4s.SchemaFor[Test](avroTestSchema)
//      val schema = com.sksamuel.avro4s.AvroSchema[Test](schemaFor)
//
//      val outputStream = AvroInputStream.binary[Test].from(data).build(schema)
//      outputStream.tryIterator.toList

    }


  }
}
