package com.sksamuel.avro4s

import org.apache.avro.Schema

trait Schema2 {
  def schema[T]: Schema
}

object Schema2 {
  
  import scala.quoted._
  
  inline def apply[T]: Unit = ${ apply[T]('[T]) }

  def apply[T](t: Type[T])(using q: Quotes) = '{
    // access the java class of T or scala 2 style type tag
  }
}



