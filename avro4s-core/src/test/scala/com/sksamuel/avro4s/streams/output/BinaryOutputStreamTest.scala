package com.sksamuel.avro4s.streams.output

import java.io.ByteArrayOutputStream
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, Decoder, Encoder, SchemaFor}
import org.apache.avro.AvroTypeException
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Failure

case class Work(name: String, year: Int, style: Style)
case class Composer(name: String, birthplace: String, works: Seq[Work])

class BinaryStreamsTest extends AnyWordSpec with Matchers {

  val ennio = Composer("ennio morricone", "rome", Seq(Work("legend of 1900", 1986, Style.Classical), Work("ecstasy of gold", 1969, Style.Classical)))
  val hans = Composer("hans zimmer", "frankfurt", Seq(Work("batman begins", 2007, Style.Modern), Work("dunkirk", 2017, Style.Modern)))

  final case class TestInside(b: String)
  final case class Test(a: Int, inside: Option[TestInside])

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

//    "finish with error" in {
//      val work = Test(3, None)
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.binary[Test].to(baos).build()
//
//      output.write(work)
//      output.flush()
//      output.close()
//
//
//      val schemaString = AvroSchema[TestInside].toString
//      val avroTestSchema = new org.apache.avro.Schema.Parser().parse(schemaString)
//      val schemaForIt = com.sksamuel.avro4s.SchemaFor[TestInside](avroTestSchema)
//      val schema = com.sksamuel.avro4s.AvroSchema[TestInside](schemaForIt)
//
//
//
//      val in = AvroInputStream.binary[Test].from(baos.toByteArray).build(schema)
//      //in.iterator.toList shouldBe List(ennio, hans)
//      println("-----1")
//      val it = in.tryIterator
//      println("------2")
//      val list = it.toList
//      println("------3")
//      println(list)
//      //in.close()
//
//
//
////      val testInside = com.sksamuel.avro4s.AvroSchema[TestInside].toString
////      val avroTestSchema = new org.apache.avro.Schema.Parser().parse(testInside)
////      val schemaFor = com.sksamuel.avro4s.SchemaFor[Test](avroTestSchema)
////      val schema = com.sksamuel.avro4s.AvroSchema[Test](schemaFor)
////
////      val outputStream = AvroInputStream.binary[Test].from(data).build(schema)
////      outputStream.tryIterator.toList
//
//    }

    "tryIterator finishes with failure for bad schema" in {
      val work = Work("theWorks", 2020, Style.Modern)
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Work].to(baos).build()

      output.write(List(work, work))
      output.flush()
      output.close()


      val composerSchemaString = AvroSchema[Composer].toString
      val composerSchema = new org.apache.avro.Schema.Parser().parse(composerSchemaString)
      val schemaForComposer = com.sksamuel.avro4s.SchemaFor[Composer](composerSchema)
      val schemaComposer = com.sksamuel.avro4s.AvroSchema[Composer](schemaForComposer)


      val in = AvroInputStream.binary[Work].from(baos.toByteArray).build(schemaComposer)
      val it = in.tryIterator
      it.toList match {
        case List(Failure(exception)) if exception.isInstanceOf[AvroTypeException] =>
        case _ => Assertions.fail()
      }
    }



  }
}
