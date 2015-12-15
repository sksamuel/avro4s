package com.sksamuel.avro4s

import java.io.{File, InputStream}
import java.nio.file.{Files, Path, Paths}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

object ModuleGenerator {

  import scala.collection.JavaConverters._

  def apply(in: InputStream): Seq[Module] = ModuleGenerator(new Parser().parse(in))
  def apply(file: File): Seq[Module] = ModuleGenerator(new Parser().parse(file))
  def apply(path: Path): Seq[Module] = apply(path.toFile)

  def apply(schema: Schema): Seq[Module] = {

    val types = scala.collection.mutable.Map.empty[String, Module]

    def schemaToType(schema: Schema): Type = {
      schema.getType match {
        case Schema.Type.ARRAY => ArrayType(schemaToType(schema.getElementType))
        case Schema.Type.BOOLEAN => PrimitiveType.Boolean
        case Schema.Type.BYTES if schema.getProp("logicalType") == "decimal" => PrimitiveType.BigDecimal
        case Schema.Type.BYTES => PrimitiveType.Bytes
        case Schema.Type.DOUBLE => PrimitiveType.Double
        case Schema.Type.ENUM => types.getOrElse(schema.getFullName, enumFor(schema))
        case Schema.Type.FIXED => PrimitiveType.String
        case Schema.Type.FLOAT => PrimitiveType.Float
        case Schema.Type.INT => PrimitiveType.Int
        case Schema.Type.LONG => PrimitiveType.Long
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

    def recordFor(schema: Schema): RecordType = {
      val record = RecordType(schema.getNamespace, schema.getName, Nil)
      types.put(schema.getFullName, record)
      val updated = record.copy(fields = schema.getFields.asScala.map { field =>
        FieldDef(field.name, schemaToType(field.schema))
      })
      types.put(schema.getFullName, updated)
      updated
    }

    require(schema.getType == Schema.Type.RECORD)
    recordFor(schema)
    types.values.toList
  }
}

sealed trait Type

// a module is a case class (avro record) or java enum (avro enum); always needs a name + package (namespace)
sealed trait Module extends Type {
  def namespace: String
  def name: String
}

case class RecordType(namespace: String, name: String, fields: Seq[FieldDef]) extends Module

case class EnumType(namespace: String, name: String, symbols: Seq[String]) extends Module

case class MapType(valueType: Type) extends Type

case class PrimitiveType(baseType: String) extends Type

object PrimitiveType {
  val Bytes = PrimitiveType("Array[Byte]")
  val BigDecimal = PrimitiveType("BigDecimal")
  val Float = PrimitiveType("Float")
  val Double = PrimitiveType("Double")
  val Long = PrimitiveType("Long")
  val String = PrimitiveType("String")
  val Int = PrimitiveType("Int")
  val Boolean = PrimitiveType("Boolean")
}

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
      case RecordType(namespace, name, _) => namespace + "." + name
      case EnumType(namespace, name, _) => namespace + "." + name
      case MapType(valueType) => s"Map[String, ${renderType(valueType)}]"
      case UnionType(NullType, right) => s"Option[${renderType(right)}]"
      case UnionType(left, right) => s"Either[${renderType(left)}, ${renderType(right)}]"
      case NullType => "null"
    }
  }
}


// templates contains all generated definitions grouped by file
case class Template(file: String, definition: String)


object FileRenderer {
  def render(dir: Path, template: Seq[Template]): Seq[Path] = {

    template.map { template =>

      val targetPath = dir resolve Paths.get(template.file.replace(".", File.separator))
      val packagePath = targetPath.getParent
      packagePath.toFile.mkdirs()

      val writer = Files.newBufferedWriter(targetPath)
      writer.write(template.definition)
      writer.close()
      targetPath

    }
  }
}