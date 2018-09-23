//package com.sksamuel.avro4s
//
//import java.io.ByteArrayOutputStream
//
//import org.apache.avro.file.CodecFactory
//import org.scalatest.{Matchers, WordSpec}
//
//class AvroDataOutputStreamTest extends WordSpec with Matchers {
//
//  case class Composer(name: String, birthplace: String, compositions: Seq[String])
//  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
//
//  "AvroDataOutputStream" should {
//    "include schema" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.data[Composer](baos)
//      output.write(ennio)
//      output.close()
//      new String(baos.toByteArray) should include ("birthplace")
//      new String(baos.toByteArray) should include ("compositions")
//    }
//
//    "include snappy coded in metadata when serialized with snappy" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.data[Composer](baos, CodecFactory.snappyCodec())
//      output.write(ennio)
//      output.close()
//      new String(baos.toByteArray) should include ("snappy")
//    }
//
//    "include deflate coded in metadata when serialized with deflate" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.data[Composer](baos, CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL))
//      output.write(ennio)
//      output.close()
//      new String(baos.toByteArray) should include ("deflate")
//    }
//
//    "include bzip2 coded in metadata when serialized with bzip2" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.data[Composer](baos, CodecFactory.bzip2Codec())
//      output.write(ennio)
//      output.close()
//      new String(baos.toByteArray) should include ("bzip2")
//    }
//  }
//}
