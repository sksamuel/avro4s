package com.sksamuel.avro4s

class ModuleRenderer {

  def apply(record: RecordType): String = {
    s"//auto generated code by avro4s\ncase class ${record.name}(\n" + record.fields.map(TypeRenderer.render).mkString(",\n") + "\n)"
  }

  def apply(enum: EnumType): String = {
    s"//auto generated code by avro4s\npublic enum ${enum.name}" + enum.symbols.mkString("{\n    ", ", ", "\n}")
  }
}
