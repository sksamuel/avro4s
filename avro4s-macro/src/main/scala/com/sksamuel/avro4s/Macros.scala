package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}

import scala.language.experimental.macros
import scala.reflect.macros.Context

//object Macros {
//
//  import scala.reflect.macros.Context
//
//  def toMap_impl[T: c.WeakTypeTag](c: Context) = {
//    import c.universe._
//
//    val mapApply = Select(reify(Map).tree, newTermName("apply"))
//
//    val pairs = weakTypeOf[T].declarations.collect {
//      case m: MethodSymbol if m.isCaseAccessor =>
//        val name = c.literal(m.name.decoded)
//        val value = c.Expr(Select(c.resetAllAttrs(c.prefix.tree), m.name))
//        reify(name.splice -> value.splice).tree
//    }
//
//    c.Expr[Map[String, Any]](Apply(mapApply, pairs.toList))
//  }
//}

trait AvroSchemaWriter[T] {
  def schema: org.apache.avro.Schema
}

//object SchemaTypeMapper {
//  def scalaTypeToSchemaType(klass: Class[T]): Schema.Type = {
//    Schema.create(Schema.Type.INT)
//  }
//}

object Schemas {
  def schemaType(sig: String): Schema.Type = {
    sig match {
      case "String" => Schema.Type.STRING
      case "Int" => Schema.Type.INT
      case "Long" => Schema.Type.LONG
      case "Boolean" => Schema.Type.BOOLEAN
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
  implicit def materializeWriter[T]: AvroSchemaWriter[T] = macro materializeWriterImpl[T]

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
          val fields = com.sksamuel.avro4s.Schemas.schemaFields($aa)
          s.setFields(fields.asJava)
          s
        }
      }
    """
    }
  }
}