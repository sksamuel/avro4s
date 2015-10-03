package com.sksamuel.avro4s

import java.io.{File, InputStream}
import java.nio.file.{Files, Path, Paths}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

class ClassGenerator(schema: Schema) {

  import scala.collection.JavaConverters._

  def records: Seq[TopLevelType] = {

    val types = scala.collection.mutable.Map.empty[String, TopLevelType]

    def schemaToType(schema: Schema): Type = {
      schema.getType match {
        case Schema.Type.ARRAY => ArrayType(schemaToType(schema.getElementType))
        case Schema.Type.BOOLEAN => PrimitiveType("Boolean")
        case Schema.Type.BYTES if schema.getProp("logicalType") == "decimal" => PrimitiveType("BigDecimal")
        case Schema.Type.BYTES => PrimitiveType("Array[Byte]")
        case Schema.Type.DOUBLE => PrimitiveType("Double")
        case Schema.Type.ENUM => types.getOrElse(schema.getFullName, enumFor(schema))
        case Schema.Type.FIXED => PrimitiveType("String")
        case Schema.Type.FLOAT => PrimitiveType("Float")
        case Schema.Type.INT => PrimitiveType("Int")
        case Schema.Type.LONG => PrimitiveType("Long")
        case Schema.Type.MAP => MapType(schemaToType(schema.getValueType))
        case Schema.Type.NULL => NullType
        case Schema.Type.RECORD => types.getOrElse(schema.getFullName, recordFor(schema))
        case Schema.Type.STRING => PrimitiveType("String")
        case Schema.Type.UNION => UnionType(schemaToType(schema.getTypes.get(0)), schemaToType(schema.getTypes.get(1)))
        case _ => sys.error("Unsupported field type: " + schema.getType)
      }
    }

    def enumFor(schema: Schema): EnumType = {
      val enum = EnumType(schema.getNamespace, schema.getName, schema.getEnumSymbols.asScala)
      types.put(schema.getFullName, enum)
      enum
    }

    def recordFor(schema: Schema): Record = {
      val record = Record(schema.getNamespace, schema.getName, Nil)
      types.put(schema.getFullName, record)
      val updated = record.copy(fields = schema.getFields.asScala.map { field =>
        FieldDef(field.name, schemaToType(field.schema))
      })
      types.put(schema.getFullName, updated)
      updated
    }

    require(schema.getType == Schema.Type.RECORD)
    recordFor(schema)
    types.values.toSeq
  }
}

object ClassGenerator {
  def apply(in: InputStream): Seq[TopLevelType] = new ClassGenerator(new Parser().parse(in)).records
  def apply(file: File): Seq[TopLevelType] = new ClassGenerator(new Parser().parse(file)).records
  def apply(path: Path): Seq[TopLevelType] = apply(path.toFile)
}

sealed trait Type

sealed trait TopLevelType extends Type {
  def namespace: String
  def name: String
}

case class Record(namespace: String, name: String, fields: Seq[FieldDef]) extends TopLevelType

case class EnumType(namespace: String, name: String, symbols: Seq[String]) extends TopLevelType

case class MapType(valueType: Type) extends Type

case class PrimitiveType(baseType: String) extends Type

case class ArrayType(arrayType: Type) extends Type

case class UnionType(left: Type, right: Type) extends Type

case object NullType extends Type

case class FieldDef(name: String, `type`: Type)

object TypeRenderer {
  def render(f: FieldDef): String = s"  ${f.name}: ${renderType(f.`type`)}"
  def renderType(t: Type): String = {
    t match {
      case PrimitiveType(base) => base
      case ArrayType(arrayType) => s"Seq[${renderType(arrayType)}]"
      case Record(namespace, name, _) => namespace + "." + name
      case EnumType(namespace, name, _) => namespace + "." + name
      case MapType(valueType) => s"Map[String, ${renderType(valueType)}]"
      case UnionType(NullType, right) => s"Option[${renderType(right)}]"
      case UnionType(left, right) => s"Either[${renderType(left)}, ${renderType(right)}]"
      case NullType => "null"
    }
  }
}

object StringClassRenderer {

  def render(types: Seq[TopLevelType]): String = {
    require(types.nonEmpty)
    s"package ${types.head.namespace}\n\n" + types.map {
      case r: Record => render(r)
      case e: EnumType => render(e)
    }.mkString("\n\n")
  }

  def render(enum: EnumType): String = {
    s"sealed trait ${enum.name}\n" +
      s"object ${enum.name} {\n" +
      enum.symbols.map(symbol => s"  case object $symbol extends ${enum.name}").mkString("\n") +
      "\n}"
  }

  def render(record: Record): String = {
    val isCase = if (record.fields.size <= 22) "case " else ""
    "// auto generated code by avro4s\n" +
      s"${isCase}class ${record.name}(\n" +
      record.fields.map(TypeRenderer.render).mkString(",\n") +
      "\n)"
  }
}

object FileRenderer {
  def render(dir: Path, classes: Seq[Record]): Seq[Path] = {
    classes.groupBy(_.namespace).map { case (namespace, packageClasses) =>
      val packageDir = dir.resolve(Paths.get(namespace.replace(".", File.separator)))
      packageDir.toFile.mkdirs()
      val path = packageDir.resolve(classes.head.name + ".scala")
      val writer = Files.newBufferedWriter(path)
      writer.write(StringClassRenderer.render(packageClasses))
      writer.close()
      path
    }.toList
  }
}