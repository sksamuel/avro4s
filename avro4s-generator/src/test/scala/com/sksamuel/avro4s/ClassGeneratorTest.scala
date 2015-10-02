package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

class ClassGeneratorTest extends WordSpec with Matchers {

  "com.sksamuel.avro4s.ClassGenerator" should {
    "generate class name" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      println(StringClassRenderer.render(defs))
    }
    "generate sealed trait for enums" in {
      val records = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      println(StringClassRenderer.render(records))
      records.find(_.name == "TwitterAccount").get.fields
        .find(_.name == "status").get.`type`.asInstanceOf[EnumType].values.toSet shouldBe
        Set("ACTIVE", "EXPIRED", "REVOKED", "PENDING", "DENIED")
    }
  }
}