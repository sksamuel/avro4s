package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder.FieldAssembler

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

object Macros {
  implicit def materializeWriter[T]: AvroSchemaWriter[T] = macro materializeWriterImpl[T]

  def materializeWriterImpl[T: c.WeakTypeTag](c: Context): c.Expr[AvroSchemaWriter[T]] = {

    import c.universe._
    val tpe = weakTypeOf[T]
    val companion = tpe.typeSymbol.companionSymbol

    val fields = tpe.declarations.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramss.head

    val a = SchemaBuilder.record(tpe.typeSymbol.asClass.fullName).fields()
    val b = fields.map { field =>
      val decoded = field.name.decoded
      new Schema.Field(decoded, Schema.create(Schema.Type.INT), "", null)
      q"""new org.apache.avro.Schema.Field($decoded, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), "", null)"""
    }

    c.Expr[AvroSchemaWriter[T]] { q"""
      new AvroSchemaWriter[$tpe] {
        def schema : org.apache.avro.Schema = {
          import scala.collection.JavaConverters._
          val s = org.apache.avro.Schema.createRecord(${tpe.typeSymbol.fullName}, "", ${tpe.typeSymbol.fullName}, false)
          val fields = (..$b).productIterator.asInstanceOf[scala.collection.Iterator[org.apache.avro.Schema.Field]]
          s.setFields(fields.toList.asJava)
          s
        }
      }
    """
    }
  }
}