package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.{Schema, SchemaBuilder}

import scala.quoted.Quotes
import scala.quoted.Expr
import scala.quoted.Type

object ScalaEnums:
  inline def schema[T]: SchemaFor[T] = ${ schema }

  def schema[T:Type](using quotes: Quotes): Expr[SchemaFor[T]] =
    import quotes.reflect.*

    val tpe = TypeRepr.of[T]
    val t = tpe.typeSymbol.tree
    println(t)
    println(tpe.toString)
    '{ new SchemaFor[T] {
      println("hello")
      override def schema: Schema = SchemaBuilder.builder().intType()
    } }