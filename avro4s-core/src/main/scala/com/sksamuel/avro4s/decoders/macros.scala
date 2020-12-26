package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Annotations, Names, SchemaConfiguration, SchemaFor}
import org.apache.avro.Schema

object MacroDecoder {
  import scala.quoted._
  
  inline def derive[T]: Decoder[T] = ${deriveImpl[T]}

  def deriveImpl[T](using quotes: Quotes, tpe: Type[T]): Expr[Decoder[T]] = {
    import quotes.reflect._
    
    // the symbol of the case class
    val classtpe = TypeTree.of[T].tpe
    val symbol = classtpe.typeSymbol
    val classdef = symbol.tree.asInstanceOf[ClassDef]
    val names = new Names(quotes)(classdef, symbol)

    val fields = symbol.caseFields.map { member =>
      val name = member.name
      val annos = new Annotations(quotes)(member.annotations)

      member.tree match {
        case ValDef(name, tpt, rhs) =>
          // valdef.tpt is a TypeTree
          // valdef.tpt.tpe is the TypeRepr of this type tree
          tpt.tpe.asType match {
            case '[t] => decodeField[t](name)
          }
      }
    }
    
    '{new Decoder[T] {
      override def decode(value: Any): T = {
        ???
      }
    }}
  }
  
  def decodeField[T](name: String): Unit = {
    
  }
}