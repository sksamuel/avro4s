package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Annotations, FieldMapper, Names, SchemaConfiguration, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

object MacroEncoder {
  import scala.quoted._
  
  inline def derive[T]: Encoder[T] = ${deriveImpl[T]}

  def deriveImpl[T](using quotes: Quotes, tpe: Type[T]): Expr[Encoder[T]] = {
    import quotes.reflect._
    
    // the symbol of the case class
    val classtpe = TypeTree.of[T].tpe
    val symbol = classtpe.typeSymbol
    val classdef = symbol.tree.asInstanceOf[ClassDef]
    val names = new Names(quotes)(classdef, symbol)
    println(symbol.caseFields)
    val fields = symbol.caseFields.map { member =>
      val name = member.name
//      val annos = new Annotations(quotes)(member.annotations)
//
//      member.tree match {
//        case ValDef(name, tpt, rhs) =>
//          // valdef.tpt is a TypeTree
//          // valdef.tpt.tpe is the TypeRepr of this type tree
//          tpt.tpe.asType match {
//            case '[t] => encodeField[t](name)
//          }
//      }
//
      Expr(name)
    }
    
    println("declaredFields=" + symbol.declaredFields)
    println("declaredFields=" + symbol.declaredFields.map(_.tree.isExpr))
    
    // val encoders = symbol.caseFields.map { member => encodeField(member.name) }
    
    val e = Varargs(fields)
    
    def getFieldValue(caseClassTerm: Term, field: Symbol): Expr[Any] = {
      val fieldValDef: ValDef = field.tree.asInstanceOf[ValDef]  // TODO remove cast
      val fieldTpe = fieldValDef.tpt.tpe
      val fieldName = fieldValDef.name

      val fieldValue: Term = Select(caseClassTerm, field)  // t.field
      fieldValue.asExpr
    }

    def putFields(t: Expr[T], record: Expr[GenericData.Record], schema: Expr[Schema]): Expr[Any] = {
      val classTerm: Term = t.asTerm
      Expr.ofList(symbol.caseFields.map { member =>
        member.tree match {
          case ValDef(name, tpt, rhs) =>
            // valdef.tpt is a TypeTree
            // valdef.tpt.tpe is the TypeRepr of this type tree
            tpt.tpe.asType match {
              case '[f] => putField[f] (member, classTerm, record, schema)
            }
        }
      })
      //      Expr.ofList(symbol.caseFields.map { member => putField[]
      //      Select(classTerm, field).asExpr
      //      })
    }
    
    def putField[F](field: Symbol, classTerm: Term, record: Expr[GenericData.Record], schema: Expr[Schema])(using quotes: Quotes, f: Type[F]): Expr[Any] = {

      val encoder: Expr[Encoder[F]] = Expr.summon[Encoder[F]] match {
        case Some(encoder) => encoder
        case _ => report.error(s"Could not find Encoder[$f] to encode field ${field.name}"); '{???}
      }

      val value = getFieldValue(classTerm, field)

      '{
        val s = $schema
        val name = ${Expr(field.name)}
        val fieldSchema = s.getField(name).schema()
        val encoded = ${encoder}.encode(fieldSchema)($value.asInstanceOf[F])
        ${record}.put(${Expr(field.name)}, encoded)
      }
    }
    
    '{new Encoder[T] {
      override def encode(schema: Schema, mapper: FieldMapper): T => Any = { t =>
         val record = new GenericData.Record(schema)
         ${putFields('{t}, '{record}, '{schema})}
         record
      }
    }}
  }

  /**
   * Encodes a field of a case class by delegating to an implicit encoder for the field's type.
   *
   * Returns a tuple of the field name to
   */
  def encodeField[T](name: String)(using quotes: Quotes, t: Type[T]): Expr[(String, Encoder[T])] = {
    import quotes.reflect._

    val encoder: Expr[(String, Encoder[T])] = Expr.summon[Encoder[T]] match {
      case Some(encoder) => '{ (${Expr(name)}, $encoder) }
      case _ => report.error(s"Could not find Encoder[$t] to encode field $name"); '{???}
    }

    encoder
  }
}