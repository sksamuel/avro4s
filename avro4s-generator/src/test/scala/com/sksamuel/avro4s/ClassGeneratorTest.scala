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
    "generate field for arrays of doubles" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/arraydouble.avsc"))
      StringClassRenderer.render(types) should include("myarray: Seq[Double]")
    }
    "generate field for arrays of longs" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/arraylong.avsc"))
      StringClassRenderer.render(types) should include("myarray: Seq[Long]")
    }
    "generate field for arrays of records" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/arrayrec.avsc"))
      StringClassRenderer.render(types) should include("bibble: com.example.avro.NestedRecord")
    }
    "generate field for bytes" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      StringClassRenderer.render(types) should include("photo: Array[Byte]")
    }
    "generate BigDecimal field for decimal logical type" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/decimal.avsc"))
      StringClassRenderer.render(types) should include("decimal: BigDecimal")
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
    "generate option for nullable unions" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      StringClassRenderer.render(types) should include("description: Option[String]")
      StringClassRenderer.render(types) should include("dateBounced: Option[Long]")
    }
    "generate either for union types" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      StringClassRenderer.render(types) should include("snoozeDate: Either[Double, Long]")
    }
  }
}