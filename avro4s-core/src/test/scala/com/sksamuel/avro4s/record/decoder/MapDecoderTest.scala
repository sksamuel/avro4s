package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s._
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class MapDecoderTest extends AnyWordSpec with Matchers {
  "Decoder" should {
    "support top level Map[String, Double]" in {
      val schema = AvroSchema[Map[String, Double]]
      val data = Map("a" -> 1.2, "ç" -> 34.5, "阿夫罗" -> 54.3)
      Decoder[Map[String, Double]].decode(schema).apply(data.asJava) shouldBe data
    }
    "support top level Map[String, Int]" in {
      val schema = AvroSchema[Map[String, Int]]
      val data = Map("a" -> 111, "ç" -> 222, "阿夫罗" -> 333)
      Decoder[Map[String, Int]].decode(schema).apply(data.asJava) shouldBe data
    }
    "support top level Map[String, String]" in {
      val schema = AvroSchema[Map[String, String]]
      val data = Map("a" -> "b", "ç" -> "đ", "阿夫罗" -> "아브로")
      val enc = data.map { case (k, v) => (k, new Utf8(v)) }.asJava
      Decoder[Map[String, String]].decode(schema).apply(enc) shouldBe data
    }
    "support top level Map[String, Seq[String]]" in {
      val schema = AvroSchema[Map[String, Seq[String]]]
      val data = Map("a" -> Seq("a", "b"), "ç" -> Seq("c", "d"), "阿夫罗" -> Seq("아브로", "f"))
      val enc = data.map { case (k, v) =>
        (k, v.map(new Utf8(_)).asJava)
      }.asJava
      Decoder[Map[String, Seq[String]]].decode(schema).apply(enc) shouldBe data
    }
    "support top level Map[String, Test]" in {
      case class Test(set: Set[String])
      val valueSchema = AvroSchema[Test]
      val schema = AvroSchema[Map[String, Test]]
      val data = Map("a" -> Test(Set("a", "b")), "ç" -> Test(Set("c", "d")), "阿夫罗" -> Test(Set("아브로", "f")))
      val enc = data.map { case (k, Test(v)) =>
        (k, ImmutableRecord(valueSchema, Seq(v.map(new Utf8(_)).toSeq.asJava)))
      }.asJava
      Decoder[Map[String, Test]].decode(schema).apply(enc) shouldBe data
    }
  }
}