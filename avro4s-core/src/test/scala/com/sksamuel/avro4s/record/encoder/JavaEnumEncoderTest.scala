//package com.sksamuel.avro4s.record.encoder
//
//import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, SchemaEncoder}
//import com.sksamuel.avro4s.schema.Wine
//import org.scalatest.{Matchers, WordSpec}
//
//class JavaEnumEncoderTest extends WordSpec with Matchers {
//
//  "Encoder" should {
//    "encode java enums" in {
//      val schema = SchemaEncoder[Test].encode()
//      Encoder[Test].encode(Test(Wine.Malbec), schema) shouldBe InternalRecord(schema, Vector(Wine.Malbec))
//    }
//    "support optional java enums" in {
//      case class Test(wine: Option[Wine])
//      val schema = SchemaEncoder[Test].encode()
//      //      Encoder[Test].encode(Test(Some(Wine.Malbec)), schema) shouldBe InternalRecord(schema, Vector(Wine.Malbec))
//      //      Encoder[Test].encode(Test(None), schema) shouldBe InternalRecord(schema, Vector(null))
//    }
//  }
//}
//
//case class Test(wine: Wine)
