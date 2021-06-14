package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroSchema, Decoder, TypeGuardedDecoding}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class GithubIssue545 extends AnyWordSpec with Matchers {
  "TypeGuardedDecoding" should {
    "create a map decoder instead of an array decoder" in {

      val mapDecoder = Decoder[Map[String, String]]
      val schema = AvroSchema[Map[String, String]]
      val typeGuard = TypeGuardedDecoding[Map[String, String]].guard(schema)
      val value = Map().asJava

      typeGuard.isDefinedAt(value) shouldBe true
    }
  }
}
