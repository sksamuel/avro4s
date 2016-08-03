package com.sksamuel.avro4s

import java.io.File

/**
  * Splits modules into templates (file name + definition)
  */
object TemplateGenerator {

  val renderer = new ModuleRenderer()

  def apply(modules: Seq[Module]): Seq[Template] = {

    // each enum must go into its own template
    val enums = modules.collect {
      case enum: EnumType => Template(
        enum.namespace.replace(".", File.separator) + File.separator + enum.name,
        "java",
        s"package ${enum.namespace};\n\n" + renderer(enum))
    }

    // records can be grouped into a single file per package
    val records = modules.collect {
      case record: RecordType => record
    }.groupBy(_.namespace).map { case (namespace, records) =>
      val defin = s"package $namespace\n\n" + records.map(renderer.apply).mkString("\n\n")
      Template(
        namespace.replace(".", File.separator) + File.separator + "domain",
        "scala",
        defin
      )
    }

    enums ++ records
  }
}
