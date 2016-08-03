package com.sksamuel.avro4s

import org.scalatest.{WordSpec, Matchers}

class TemplateGeneratorTest extends WordSpec with Matchers {

  "TemplateGenerator" should {
    "generate a file per enum" in {
      val enums = TemplateGenerator(Seq(EnumType("com.a", "Boo", Seq("A", "B")), EnumType("com.a", "Foo", Seq("A", "B"))))
      enums shouldBe Seq(Template("com/a/Boo", "java", "package com.a;\n\n//auto generated code by avro4s\npublic enum Boo{\n    A, B\n}"), Template("com/a/Foo", "java", "package com.a;\n\n//auto generated code by avro4s\npublic enum Foo{\n    A, B\n}"))
    }
    "generate one file for all records of same namespace" in {
      val enums = TemplateGenerator(Seq(RecordType("com.a", "Boo", Seq(FieldDef("name", PrimitiveType.String), FieldDef("bibble", PrimitiveType.Long))), RecordType("com.a", "Foo", Seq(FieldDef("dibble", PrimitiveType.Double), FieldDef("bibble", PrimitiveType.Boolean)))))
      enums shouldBe Seq(Template("com/a/domain", "scala", "package com.a\n\n//auto generated code by avro4s\ncase class Boo(\n  name: String,\n  bibble: Long\n)\n\n//auto generated code by avro4s\ncase class Foo(\n  dibble: Double,\n  bibble: Boolean\n)"))
    }
  }
}
