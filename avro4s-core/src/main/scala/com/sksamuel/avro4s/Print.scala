//package com.sksamuel.avro4s
//
//import scala.deriving.Mirror
//
//trait Print[T] {
//  def print: String
//}
//
//object Print {
//  
//  def apply[T](using p: Print[T]): Print[T] = p
//
//  given Print[String] = new Print[String] {
//    override def print: String = "string"
//  }
//  
//  inline given derived[T](using m: Mirror.Of[T]) : Print[T] = PrintMacros.derive[T]
//}