package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

class ClassGeneratorTest extends WordSpec with Matchers {

  "com.sksamuel.avro4s.ClassGenerator" should {
    "generate package name" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(defs) should include("package com.sksamuel.avro4s")
    }
    "generate class name" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(defs) should include("GameOfThrones")
    }
    "generate case class for records" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(defs) should include("case class GameOfThrones")
    }
    "generate field for Int fields" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(defs) should include("kingdoms: Int")
    }
    "generate field for Boolean fields" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(defs) should include("aired: Boolean")
    }
    "generate field for Double fields" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(types) should include("temperature: Double")
    }
    "generate field for String fields" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(types) should include("id: String")
    }
    "generate field for Long fields" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      StringClassRenderer.render(types) should include("deathCount: Long")
    }
    "generate field for Maps with primitives" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/map.avsc"))
      StringClassRenderer.render(types) should include("mymap: Map[String, Long]")
    }
    "generate field for arrays of strings" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/array.avsc"))
      StringClassRenderer.render(types) should include("myarray: Seq[String]")
    }
    "generate definition for enums" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      println(StringClassRenderer.render(types))
      types.find(_.name == "TwitterAccount").get.asInstanceOf[Record].fields
        .find(_.name == "status").get.`type`.asInstanceOf[EnumType].symbols.toSet shouldBe
        Set("ACTIVE", "EXPIRED", "REVOKED", "PENDING", "DENIED")
    }
    "generate sealed trait for enums" in {
      val records = ClassGenerator(getClass.getResourceAsStream("/enum.avsc"))
      StringClassRenderer.render(records) should include("sealed trait MyEnum")
      StringClassRenderer.render(records) should include("case object DONE extends MyEnum")
      StringClassRenderer.render(records) should include("case object ARCHIVED extends MyEnum")
      StringClassRenderer.render(records) should include("case object DELETED extends MyEnum")
    }
  }
}