package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.{Annotations, Names, SchemaConfiguration}
import org.apache.avro.{Schema, SchemaBuilder}
import scala.quoted._

import scala.collection.mutable.WeakHashMap

object Macros {

  import scala.collection.convert.AsJavaConverters
  import scala.compiletime.{constValue, constValueOpt, erasedValue, summonInline}

  inline def derive[T]: SchemaFor[T] = ${deriveImpl[T]}

  def deriveImpl[T](using quotes: Quotes, tpe: Type[T]): Expr[SchemaFor[T]] = {
    import quotes.reflect._

    // the symbol of the case class
    val classtpe = TypeTree.of[T].tpe
    val symbol = classtpe.typeSymbol
    val classdef = symbol.tree.asInstanceOf[ClassDef]
    val names = new Names(quotes)(classdef, symbol)

    val defaultNamespace = names.namespace
//    println("default = "+ defaultNamespace)

    // annotations on the case class
    val annos = new Annotations(quotes)(symbol.annotations)
    val error = annos.error
    val doc: Option[String] = annos.doc
    val namespace: String = annos.namespace.map(_.replaceAll("[^a-zA-Z0-9_.]", "")).getOrElse(names.namespace)

    // the short name of the class
    val simpleName: String = symbol.name
    val fullName: String = symbol.fullName

    // TypeTree.of[V].symbol.declaredFields

    val fields = symbol.caseFields.map { member =>
      val name = member.name
      val annos = new Annotations(quotes)(member.annotations)

      member.tree match {
        case ValDef(name, tpt, rhs) =>
          // valdef.tpt is a TypeTree
          // valdef.tpt.tpe is the TypeRepr of this type tree
          tpt.tpe.asType match {
             case '[f] => field[f](name, annos.doc, annos.namespace.getOrElse(namespace))
          }
      }
    }
    
    val e = Varargs(fields)
    
    '{
        Records.record[T](
          name = ${Expr(simpleName)},
          doc = ${Expr(doc)}, 
          namespace = ${Expr(namespace)}, 
          isError = ${Expr(error)}, 
          tofields = $e
        )
    }
  }

  def field[F](name: String, doc: Option[String], namespace: String)(using quotes: Quotes, t: Type[F]): Expr[ToField] = {
    import quotes.reflect._

    val schemaFor: Expr[SchemaFor[F]] = Expr.summon[SchemaFor[F]] match {
      case Some(schemaFor) => 
        println(s"found schema for $schemaFor")
        schemaFor
      case _ => report.error(s"Could not find schemaFor for $t"); '{???}
    }
    
    '{
      Records.field[F](
        name = ${Expr(name)},
        doc = ${Expr(doc)},
        namespace = ${Expr(namespace)},
        schemaFor = $schemaFor
      )
    }
  }
}
