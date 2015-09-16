package com.sksamuel.avro4s

import org.apache.avro.Schema

import scala.language.experimental.macros
import scala.reflect.macros.Context

trait AvroSchemaWriter[T] {
  def schema: org.apache.avro.Schema
}

object SchemaUtils {

  def schemaType(sig: String): Schema.Type = {
    sig match {
      case "Array[Byte]" | "List[Byte]" | "Seq[Byte]" => Schema.Type.BYTES
      case "Array" => Schema.Type.ARRAY
      case "Boolean" => Schema.Type.BOOLEAN
      case "Double" => Schema.Type.DOUBLE
      case "Float" => Schema.Type.FLOAT
      case "Int" => Schema.Type.INT
      case "Long" => Schema.Type.LONG
      case "String" => Schema.Type.STRING
      case _ => Schema.Type.STRING
    }
  }

  def schemaFields(xs: Seq[(String, String)]): List[Schema.Field] = {
    xs.map { case (name, sig) =>
      new Schema.Field(name, Schema.create(schemaType(sig)), s"Generated from scala type: $sig", null)
    }.toList
  }
}

object Macros {
  def materializeWriterImpl[T: c.WeakTypeTag](c: Context): c.Expr[AvroSchemaWriter[T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]
    val companion = tpe.typeSymbol.companionSymbol

    val fields = tpe.declarations.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramss.head

    val aa = fields.map { field => field.name.decoded -> field.typeSignature.toString }

    c.Expr[AvroSchemaWriter[T]] { q"""
      new AvroSchemaWriter[$tpe] {
        def schema : org.apache.avro.Schema = {
          import scala.collection.JavaConverters._
          val s = org.apache.avro.Schema.createRecord(${tpe.typeSymbol.fullName}, "", ${tpe.typeSymbol.fullName}, false)
          val fields = com.sksamuel.avro4s.SchemaUtils.schemaFields($aa)
          s.setFields(fields.asJava)
          s
        }
      }
    """
    }
  }
}