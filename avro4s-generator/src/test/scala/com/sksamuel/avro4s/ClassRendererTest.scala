package com.sksamuel.avro4s

import org.scalatest.{WordSpec, Matchers}

class ClassRendererTest extends WordSpec with Matchers {

  val types = ModuleGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
  val fields = types.collect {
    case record: RecordType => record.fields
  }.flatten

  "ClassRenderer" should {
    "generate field for Int fields" in {
      fields should contain(FieldDef("kingdoms", PrimitiveType("Int")))
    }
    "generate field for Boolean fields" in {
      fields should contain(FieldDef("aired", PrimitiveType("Boolean")))
    }
    "generate field for Double fields" in {
      fields should contain(FieldDef("temperature", PrimitiveType("Double")))
    }
    "generate field for String fields" in {
      fields should contain(FieldDef("ruler", PrimitiveType("String")))
    }
    "generate field for Long fields" in {
      fields should contain(FieldDef("deathCount", PrimitiveType("Long")))
    }
    "generate definition for enums" in {
      val enum = ModuleGenerator(getClass.getResourceAsStream("/enum.avsc")).head
      enum shouldBe EnumType("com.example.avro", "MyEnum", List("DONE", "ARCHIVED", "DELETED"))
    }
  }
}
