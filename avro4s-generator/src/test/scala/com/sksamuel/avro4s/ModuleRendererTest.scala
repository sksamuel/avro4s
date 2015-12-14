package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

class ModuleRendererTest extends WordSpec with Matchers {

  "ModuleRenderer" should {
    "generate class name" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      ModuleRenderer.render(defs) should include("GameOfThrones")
    }
    "generate case class for records" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      ModuleRenderer.render(defs) should include("case class GameOfThrones")
    }
    "generate field for Int fields" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      ModuleRenderer.render(defs) should include("kingdoms: Int")
    }
    "generate field for Boolean fields" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      ModuleRenderer.render(defs) should include("aired: Boolean")
    }
    "generate field for Double fields" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      ModuleRenderer.render(types) should include("temperature: Double")
    }
    "generate field for String fields" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      ModuleRenderer.render(types) should include("id: String")
    }
    "generate field for Long fields" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      ModuleRenderer.render(types) should include("deathCount: Long")
    }
    "generate field for Maps with primitives" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/map.avsc"))
      ModuleRenderer.render(types) should include("mymap: Map[String, Long]")
    }
    "generate field for arrays of strings" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/array.avsc"))
      ModuleRenderer.render(types) should include("myarray: Seq[String]")
    }
    "generate field for arrays of doubles" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/arraydouble.avsc"))
      ModuleRenderer.render(types) should include("myarray: Seq[Double]")
    }
    "generate field for arrays of longs" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/arraylong.avsc"))
      ModuleRenderer.render(types) should include("myarray: Seq[Long]")
    }
    "generate field for arrays of records" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/arrayrec.avsc"))
      ModuleRenderer.render(types) should include("bibble: com.example.avro.NestedRecord")
    }
    "generate field for bytes" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      ModuleRenderer.render(types) should include("photo: Array[Byte]")
    }
    "generate BigDecimal field for decimal logical type" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/decimal.avsc"))
      ModuleRenderer.render(types) should include("decimal: BigDecimal")
    }
    "generate definition for enums" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      println(ModuleRenderer.render(types))
      types.find(_.name == "TwitterAccount").get.asInstanceOf[Record].fields
        .find(_.name == "status").get.`type`.asInstanceOf[EnumType].symbols.toSet shouldBe
        Set("ACTIVE", "EXPIRED", "REVOKED", "PENDING", "DENIED")
    }
    "generate java enum for enums" in {
      val records = ClassGenerator(getClass.getResourceAsStream("/enum.avsc"))
      ModuleRenderer.render(records) should include("public enum MyEnum{\n    DONE, ARCHIVED, DELETED\n}")
    }
    "generate option for nullable unions" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      ModuleRenderer.render(types) should include("description: Option[String]")
      ModuleRenderer.render(types) should include("dateBounced: Option[Long]")
    }
    "generate either for union types of two" in {
      val types = ClassGenerator(getClass.getResourceAsStream("/user.avsc"))
      ModuleRenderer.render(types) should include("snoozeDate: Either[Double, Long]")
    }
  }
}