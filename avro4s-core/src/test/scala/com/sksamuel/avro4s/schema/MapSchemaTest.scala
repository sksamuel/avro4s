package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}

class MapSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "generate map type for a scala.collection.immutable.Map of primitives" in {
      case class Test(map: Map[String, String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/map.avsc"))
      val schema = SchemaEncoder[Test].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate map type for a scala.collection.immutable.Map of records" in {
      case class Nested(goo: String)
      case class Test(map: Map[String, Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/maprecord.avsc"))
      val schema = SchemaEncoder[Test].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate map type for a scala.collection.immutable.Map of Option[Boolean]" in {
      case class Test(map: Map[String, Option[Boolean]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/mapoption.avsc"))
      val schema = SchemaEncoder[Test].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support maps of seqs of records" in {
      case class Nested(goo: String)
      case class Test(map: Map[String, Seq[Nested]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/map_seq_nested.avsc"))
      val schema = SchemaEncoder[Test].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
  }

}
