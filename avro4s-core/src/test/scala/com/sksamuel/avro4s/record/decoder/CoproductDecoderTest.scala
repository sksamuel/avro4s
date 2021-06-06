//package com.sksamuel.avro4s.record.decoder
//
//import java.nio.ByteBuffer
//
//import com.sksamuel.avro4s.record.decoder.CPWrapper.{ISBG, ISCB}
//import com.sksamuel.avro4s.{AvroSchema, Decoder}
//import org.apache.avro.generic.GenericData
//import org.apache.avro.util.Utf8
//import shapeless.{:+:, CNil, Coproduct}
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//import scala.collection.JavaConverters._
//
//class CoproductDecoderTest extends AnyFunSuite with Matchers {
//
//  test("coproducts with primitives") {
//    val decoder = Decoder[CPWrapper]
//    val record = new GenericData.Record(decoder.schema)
//    record.put("u", new Utf8("wibble"))
//    decoder.decode(record) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG]("wibble"))
//  }
//
//  test("coproducts with case classes") {
//    val decoder = Decoder[CPWrapper]
//    val gimble = new GenericData.Record(AvroSchema[Gimble])
//    gimble.put("x", new Utf8("foo"))
//    val record = new GenericData.Record(decoder.schema)
//    record.put("u", gimble)
//    decoder.decode(record) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo")))
//  }
//
//  test("coproducts with options") {
//    val codec = Decoder[CPWithOption]
//    val gimble = new GenericData.Record(AvroSchema[Gimble])
//    gimble.put("x", new Utf8("foo"))
//    val record = new GenericData.Record(codec.schema)
//    record.put("u", gimble)
//    codec.decode(record) shouldBe CPWithOption(Some(Coproduct[CPWrapper.ISBG](Gimble("foo"))))
//  }
//
//  test("coproduct with array") {
//    val schema = AvroSchema[CPWithArray]
//    val array = new GenericData.Array(AvroSchema[Seq[String]], List(new Utf8("a"), new Utf8("b")).asJava)
//    val record = new GenericData.Record(schema)
//    record.put("u", array)
//    Decoder[CPWithArray].decode(record) shouldBe CPWithArray(Coproduct[CPWrapper.SSI](Seq("a", "b")))
//  }
//
//  test("coproduct with byte array") {
//    val schema = AvroSchema[CPWithByteArray]
//    val record = new GenericData.Record(schema)
//    record.put("u", ByteBuffer.wrap(Array[Byte](1, 2, 3)))
//
//    val result = Decoder[CPWithByteArray].decode(record)
//
//    result.u.select[Array[Byte]].get shouldBe Array[Byte](1, 2, 3)
//  }
//
//  test("coproduct with array of coproducts") {
//    val coproduct = CoproductWithArrayOfCoproduct(Coproduct[ISCB](Seq(Coproduct[ISBG](3), Coproduct[ISBG]("three"))))
//    val array = new GenericData.Array(AvroSchema[Seq[ISBG]], List(3, new Utf8("three")).asJava)
//    val record = new GenericData.Record(AvroSchema[CoproductWithArrayOfCoproduct])
//    record.put("union", array)
//    Decoder[CoproductWithArrayOfCoproduct].decode(record) shouldBe coproduct
//  }
//
//  test("coproducts") {
//    val schema = AvroSchema[Coproducts]
//    val record = new GenericData.Record(schema)
//    record.put("union", new Utf8("foo"))
//    val coproduct = Coproduct[Int :+: String :+: Boolean :+: CNil]("foo")
//    Decoder[Coproducts].decode(record) shouldBe Coproducts(coproduct)
//  }
//
//  test("coproducts of coproducts") {
//    val schema = AvroSchema[CoproductsOfCoproducts]
//    val record = new GenericData.Record(schema)
//    record.put("union", new Utf8("foo"))
//    val coproduct = Coproduct[(Int :+: String :+: CNil) :+: Boolean :+: CNil](Coproduct[Int :+: String :+: CNil]("foo"))
//    Decoder[CoproductsOfCoproducts].decode(record) shouldBe CoproductsOfCoproducts(coproduct)
//  }
//}
//
//case class CPWithArray(u: CPWrapper.SSI)
//
//case class CPWithByteArray(u: CPWrapper.BAI)
//
//case class Gimble(x: String)
//case class CPWrapper(u: CPWrapper.ISBG)
//case class CPWithOption(u: Option[CPWrapper.ISBG])
//
//object CPWrapper {
//  type ISBG = Int :+: String :+: Boolean :+: Gimble :+: CNil
//  type SSI = Seq[String] :+: Int :+: CNil
//  type ISCB = Int :+: Seq[ISBG] :+: String :+: CNil
//  type BAI = Array[Byte] :+: Int :+: CNil
//}
//
//case class Coproducts(union: Int :+: String :+: Boolean :+: CNil)
//case class CoproductsOfCoproducts(union: (Int :+: String :+: CNil) :+: Boolean :+: CNil)
//case class CoproductWithArrayOfCoproduct(union: CPWrapper.ISCB)
