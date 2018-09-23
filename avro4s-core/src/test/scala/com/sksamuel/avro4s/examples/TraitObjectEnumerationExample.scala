//package com.sksamuel.avro4s.examples
//
//import org.scalatest.{Matchers, WordSpec}
//
//import scala.util.Failure
//
///**
//  *
//  * Example of how to map sealed trait+case object style of enumeration to
//  * avro.
//  *
//  * API inspired by scodec
//  *
//  * Design goals were to avoid the three individual implicits required by README "custom type mapping" example
//  * and avoid redundant specification of the object/enum mapping.
//  *
//  */
//class TraitObjectEnumerationExample extends WordSpec with Matchers {
//
//  sealed trait Base
//
//  case object A extends Base
//
//  case object B extends Base
//
//  case class Test(v: Base)
//
//
//  import scala.util.Success
//  import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
//  import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema}
//
//  "AvroStream" should {
//
//    "generate schema using enum" in {
//      AvroSchema[Test].toString shouldBe
//        """{"type":"record","name":"Test","namespace":"com.sksamuel.avro4s.examples","fields":[{"name":"v","type":{"type":"enum","name":"Base","symbols":["A","B"]}}]}"""
//    }
//
//    "serialize as string" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.json[Test](baos)
//      output.write(Test(A))
//      output.close()
//      baos.toString("UTF-8") shouldBe """{"v":"A"}"""
//    }
//
//    "deserialize from string" in {
//      val json = """{"v":"B"}"""
//      val in = new ByteArrayInputStream(json.getBytes("UTF-8"))
//      val input = AvroInputStream.json[Test](in)
//      input.singleEntity shouldBe Success(Test(B))
//    }
//
//    "handle deserialization from invalid message at runtime" in {
//      val json = """{"v":"C"}"""
//      val in = new ByteArrayInputStream(json.getBytes("UTF-8"))
//      val input = AvroInputStream.json[Test](in)
//      input.singleEntity.asInstanceOf[Failure[Test]].exception.getMessage shouldBe "Unknown symbol in enum C"
//    }
//  }
//}
