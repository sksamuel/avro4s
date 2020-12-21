package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}

object SchemaForMacros {

  import scala.quoted._
  import scala.compiletime.{erasedValue, summonInline, constValue, constValueOpt}
  import scala.collection.convert.AsJavaConverters

  inline def derive[T]: SchemaFor[T] = ${deriveImpl[T]}

  def deriveImpl[T](using quotes: Quotes, tpe: Type[T]): Expr[SchemaFor[T]] = {
    import quotes.reflect._

    // the symbol of the case class
    val classtpe = TypeTree.of[T].tpe
    val symbol = classtpe.typeSymbol
    val classdef = symbol.tree.asInstanceOf[ClassDef]
    val names = new Names(quotes)(classdef, symbol)

    val defaultNamespace = names.namespace
    println("default = "+ defaultNamespace)
    
    // annotations on the case class
    val annos = new Annotations(quotes)(symbol.annotations)
    val error = annos.error
    val doc: Option[String] = annos.doc
    val namespace: String = annos.namespace.map(_.replaceAll("[^a-zA-Z0-9_.]", "")).getOrElse(names.namespace)

    // the short name of the class
    val className: String = symbol.name
    
    // TypeTree.of[V].symbol.declaredFields

    val fields = symbol.caseFields.map { member =>
      val name = member.name
      val annos = new Annotations(quotes)(member.annotations)
      
      member.tree match {
        case ValDef(name, tpt, rhs) =>
          // valdef.tpt is a TypeTree
          // valdef.tpt.tpe is the TypeRepr of this type tree
          tpt.tpe.asType match {
             case '[t] => field[t](name, annos.doc, annos.namespace)
          }
      }
    }
    val e = Varargs(fields)
    // println(e)
    
    '{new SchemaFor[T] {

      val javafields = new java.util.ArrayList[Schema.Field]()
      $e.foreach { field => javafields.add(field) }
      
      val _schema = Schema.createRecord(${Expr(className)}, ${Expr(doc)}.orNull, ${Expr(namespace)}, ${Expr(error)}, javafields)
      override def schema[T]: Schema = _schema
    }}
  }

  def field[T](name: String, doc: Option[String], namespace: Option[String])(using quotes: Quotes, t: Type[T]): Expr[Schema.Field] = {
    import quotes.reflect._
    
    val schemaFor: Expr[SchemaFor[T]] = Expr.summon[SchemaFor[T]] match {
      case Some(schemaFor) => schemaFor
      case _ => report.error(s"Could not find schemaFor for $t"); '{???}
    }
    
    '{
        new Schema.Field(${Expr(name)}, ${schemaFor}.schema, ${Expr(doc)}.getOrElse(null)) 
     }
  }
}

case class FieldRef(name: String)