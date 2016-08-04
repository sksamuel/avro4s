package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

class ModuleRendererTest extends WordSpec with Matchers {

  "new ModuleRenderer()" should {
    "write field for Int" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("foo", PrimitiveType.String)))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  foo: String\n)"
    }
    "write field for String" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("foo", PrimitiveType.Int)))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  foo: Int\n)"
    }
    "write field for Boolean" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("foo", PrimitiveType.Boolean)))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  foo: Boolean\n)"
    }
    "write field for doubles" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("foo", PrimitiveType.Double)))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  foo: Double\n)"
    }
    "write field for longs" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("foo", PrimitiveType.Long)))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  foo: Long\n)"
    }
    "generate field for Maps with strings" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", MapType(PrimitiveType.String))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Map[String, String]\n)"
    }
    "generate field for Maps with doubles" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", MapType(PrimitiveType.Double))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Map[String, Double]\n)"
    }
    "generate field for arrays of strings" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", ArrayType(PrimitiveType.String))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Seq[String]\n)"
    }
    "generate field for arrays of doubles" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", ArrayType(PrimitiveType.Double))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Seq[Double]\n)"
    }
    "generate field for arrays of longs" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", ArrayType(PrimitiveType.Long))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Seq[Long]\n)"
    }
    "generate field for arrays of records" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", ArrayType(RecordType("com.sammy", "NestedClass", Seq(FieldDef("name", PrimitiveType.String)))))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Seq[com.sammy.NestedClass]\n)"
    }
    "generate field for bytes" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", PrimitiveType.Bytes)))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Array[Byte]\n)"
    }
    "generate BigDecimal field" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", PrimitiveType.BigDecimal)))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: BigDecimal\n)"
    }
    "generate java enum for enums" in {
      new ModuleRenderer()(EnumType("com.sammy", "MyClass", Seq("Boo", "Foo", "Hoo"))) shouldBe "//auto generated code by avro4s\npublic enum MyClass{\n    Boo, Foo, Hoo\n}"
    }
    "generate option for nullable unions" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", UnionType(NullType, PrimitiveType.String))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Option[String]\n)"
    }
    "generate either for union types of records" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", UnionType(PrimitiveType.String, RecordType("com.sammy", "NestedClass", Seq(FieldDef("name", PrimitiveType.String)))))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Either[String, com.sammy.NestedClass]\n)"
    }
    "generate Option[Either] for union types of 3 types with null" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", UnionType(NullType, PrimitiveType.Int, PrimitiveType.Boolean))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Option[Either[Int, Boolean]]\n)"
    }
    "generate coproducts for union types of 3+ non-null types" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", UnionType(PrimitiveType.String, PrimitiveType.Int, PrimitiveType.Boolean))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: shapeless.:+:[String, shapeless.:+:[Int, shapeless.:+:[Boolean, shapeless.CNil]]]\n)"
    }
    "generate Option[coproducts] for union types of 3+ non-null types with null" in {
      new ModuleRenderer()(RecordType("com.sammy", "MyClass", Seq(FieldDef("name", UnionType(NullType, PrimitiveType.String, PrimitiveType.Int, PrimitiveType.Boolean))))) shouldBe "//auto generated code by avro4s\ncase class MyClass(\n  name: Option[shapeless.:+:[String, shapeless.:+:[Int, shapeless.:+:[Boolean, shapeless.CNil]]]]\n)"
    }}
}