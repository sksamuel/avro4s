//package com.sksamuel.avro4s.record.encoder
//
//import java.util.UUID
//
//import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord}
//import org.apache.avro.util.Utf8
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//
//class UUIDEncoderTest extends AnyWordSpec with Matchers {
//
//  import scala.collection.JavaConverters._
//
//  "Encoder" should {
//    "encode uuids" in {
//      val uuid = UUID.randomUUID()
//      val schema = AvroSchema[UUIDTest]
//      Encoder[UUIDTest].encode(UUIDTest(uuid)) shouldBe ImmutableRecord(schema, Vector(new Utf8(uuid.toString)))
//    }
//    "encode seq of uuids" in {
//      val uuid1 = UUID.randomUUID()
//      val uuid2 = UUID.randomUUID()
//      val schema = AvroSchema[UUIDSeq]
//      Encoder[UUIDSeq].encode(UUIDSeq(Seq(uuid1, uuid2))) shouldBe ImmutableRecord(schema, Vector(List(new Utf8(uuid1.toString), new Utf8(uuid2.toString)).asJava))
//    }
//    // todo once magnolia 2 has defaults
////    "encode UUIDs with defaults" in {
////      val uuid = UUID.randomUUID()
////      val schema = AvroSchema[UUIDDefault]
////      Encoder[UUIDDefault].encode(UUIDDefault(uuid)) shouldBe ImmutableRecord(schema, Vector(new Utf8(uuid.toString)))
////    }
//    "encode Option[UUID]" in {
//      val uuid = UUID.randomUUID()
//      val schema = AvroSchema[UUIDOption]
//      Encoder[UUIDOption].encode(UUIDOption(Some(uuid))) shouldBe ImmutableRecord(schema, Vector(new Utf8(uuid.toString)))
//      Encoder[UUIDOption].encode(UUIDOption(None)) shouldBe ImmutableRecord(schema, Vector(null))
//    }
//  }
//}
//
//case class UUIDTest(uuid: UUID)
//case class UUIDSeq(uuids: Seq[UUID])
//case class UUIDDefault(uuid: UUID = UUID.fromString("86da265c-95bd-443c-8860-9381efca059d"))
//case class UUIDOption(uuid: Option[UUID])