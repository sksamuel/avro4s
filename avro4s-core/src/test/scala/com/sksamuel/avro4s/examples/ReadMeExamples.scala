//package com.sksamuel.avro4s.examples
//
//import org.scalatest.{Matchers, WordSpec}
//
///**
//  * Tests created from README examples
//  *
//  */
//class ReadMeExamples extends WordSpec with Matchers {
//
//  import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
//
//  import com.sksamuel.avro4s.{AvroOutputStream, AvroInputStream}
//
//  case class Composer(name: String, birthplace: String, compositions: Seq[String])
//
//  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
//
//  "AvroStream binary serialization" should {
//
//    "round trip the objects " in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.binary[Composer](baos)
//      output.write(ennio)
//      output.close()
//
//      val bytes = baos.toByteArray
//
//      bytes shouldBe (Array[Byte](30, 101, 110, 110, 105, 111, 32, 109, 111, 114, 114, 105, 99, 111, 110, 101, 8, 114,
//        111, 109, 101, 4, 28, 108, 101, 103, 101, 110, 100, 32, 111, 102, 32, 49, 57, 48, 48, 30, 101, 99, 115, 116,
//        97, 115, 121, 32, 111, 102, 32, 103, 111, 108, 100, 0))
//
//      val in = new ByteArrayInputStream(bytes)
//      val input = AvroInputStream.binary[Composer](in)
//      val result = input.iterator.toSeq
//      result shouldBe Vector(ennio)
//    }
//  }
//
//  "AvroStream json serialization" should {
//
//    "round trip the objects " in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.json[Composer](baos)
//      output.write(ennio)
//      output.close()
//
//      val json = baos.toString("UTF-8")
//
//      json shouldBe ("{\"name\":\"ennio morricone\",\"birthplace\":\"rome\",\"compositions\":[\"legend of 1900\",\"ecstasy of gold\"]}")
//
//      val in = new ByteArrayInputStream(json.getBytes("UTF-8"))
//      val input = AvroInputStream.json[Composer](in)
//      val result = input.iterator.toSeq
//      result shouldBe Vector(ennio)
//    }
//  }
//
//}
