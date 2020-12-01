package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}

object SchemaForMacros extends BaseSchemas {

  import scala.quoted._
  import scala.compiletime.{erasedValue, summonInline, constValue, constValueOpt}
  import scala.collection.convert.AsJavaConverters

  inline def derive[T]: SchemaFor[T] = ${deriveImpl[T]}

  def deriveImpl[T](using quotes: Quotes, tpe: Type[T]): Expr[SchemaFor[T]] = {
    import quotes.reflect._

    // the symbol of the case class
    val symbol = TypeTree.of[T].tpe.typeSymbol

    // the short name of the class
    val className = symbol.name
    println(className)

    val classdef = symbol.tree.asInstanceOf[ClassDef]

    val fields = symbol.caseFields.map { member =>
      val name = Expr(member.name)
      member.tree match {
        case valdef: ValDef =>
          // valdef.tpt is a Tree eg TypeTree
          // valdef.tpt.tpe is the TypeRepr of this type tree
          // get the Type[?]
          // println(valdef.tpt.tpe)
          valdef.tpt.tpe.asType match {
             case '[t] => field[t](name)
          }
      }
    }
    val e = Varargs(fields)
    // println(e)
    
    '{new SchemaFor[T] {

      val javafields = new java.util.ArrayList[Schema.Field]()
      $e.foreach { field => javafields.add(field) }
      
      val _schema = Schema.createRecord(${Expr(className)}, null, "mynamespace", false, javafields)
      override def schema[T]: Schema = _schema
    }}
  }

  def field[T](using quotes: Quotes, tpe: Type[T]): Unit = {
    val fieldSchemaFor = Expr.summon[SchemaFor[T]].get
  }
  
  def field[T](name: Expr[String])(using Quotes, Type[T]): Expr[Schema.Field] = {
    println(s"Trying to find SchemaFor[${Type.show[T]}]")
    val schemaFor = schemaForLookup[SchemaFor[T]]
    println(schemaFor)
    //val schemaFor: Expr[SchemaFor[T]] = Expr.summon[SchemaFor[T]].get
    '{
        new Schema.Field($name, SchemaBuilder.builder.intType, null) 
     }
  }

  def schemaForLookup[SF](using q: Quotes, t: Type[SF]): Option[Expr[SF]] = {
    println(s"Trying to find ${Type.show[SF]}")
    return Expr.summon[SF]
  }
}

case class FieldRef(name: String)