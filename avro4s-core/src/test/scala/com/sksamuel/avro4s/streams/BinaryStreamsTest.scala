//package com.sksamuel.avro4s.streams
//
//import java.io.ByteArrayOutputStream
//
//import com.sksamuel.avro4s.internal.{AvroSchema, Encoder, SchemaEncoder}
//import com.sksamuel.avro4s.{AvroInputStream2, AvroOutputStream, AvroOutputStream2}
//import org.scalatest.{Matchers, WordSpec}
//
//case class Work(name: String, year: Int, style: Style)
//case class Composer(name: String, birthplace: String, works: Seq[Work])
//
//class BinaryStreamsTest extends WordSpec with Matchers {
//
//  val ennio = Composer("ennio morricone", "rome", Seq(Work("legend of 1900", 1986, Style.Classical), Work("ecstasy of gold", 1969, Style.Classical)))
//  val hans = Composer("hans zimmer", "frankfurt", Seq(Work("batman begins", 2007, Style.Modern), Work("dunkirk", 2017, Style.Modern)))
//
//  "Avro binary streams" should {
//    "read and write" in {
//
//      implicit val schema = AvroSchema[Composer]
//      implicit val encoder = Encoder[Composer]
//
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.binary[Composer](baos)
//      output.write(ennio)
//      output.write(hans)
//      output.close()
//
//      new String(baos.toByteArray) should not include "birthplace"
//      new String(baos.toByteArray) should not include "compositions"
//
//      val in = AvroInputStream2.binary[Composer](baos.toByteArray)
//      in.iterator.toList shouldBe List(ennio, hans)
//      in.close()
//    }
//  }
//}
