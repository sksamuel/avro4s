//package com.sksamuel.avro4s
//
//import com.sksamuel.avro4s.SchemaForMacros.deriveImpl
//import org.apache.avro.Schema
//
//trait Lazy[T] {
//  def get: T
//}
//
//class Stack(map: Map[String, Lazy[Print[_]]]) {
//  def get[T]: Option[Print[T]] = ???
//}
//
//object Stack {
//  val empty = Stack(Map.empty)
//  val threadLocalStack = ThreadLocal.withInitial[Stack](() => empty)
//}
//
//object PrintMacros {
//
//  import scala.quoted._
//  
//  inline def derive[T]: Print[T] = ${deriveImpl[T]}
//
//  def deriveImpl[T](using quotes: Quotes, tpe: Type[T]): Expr[Print[T]] = {
//    import quotes.reflect._
//
//    // the symbol of the case class
//    val classtpe = TypeTree.of[T].tpe
//    val symbol = classtpe.typeSymbol
//    val name = symbol.name
//    
//    Stack.threadLocalStack
//
//    val prints: List[Expr[Print[_]]] = symbol.caseFields.map { member =>
//      val name = member.name
//      member.tree match {
//        case ValDef(name, tpt, rhs) =>
//          // valdef.tpt is a TypeTree
//          // valdef.tpt.tpe is the TypeRepr of this type tree
//          tpt.tpe.asType match {
//            case '[t] => field[t](name)
//          }
//      }
//    }
//    
//    val e = Varargs(prints)
//    
//    '{
//       new Print[T] {
//         override def print: String = ${Expr(name)} + "(" + ${e}.map(_.print).mkString(",") + ")"
//       }
//    }
//  }
//
//
//  def field[T](name: String)(using quotes: Quotes, t: Type[T]): Expr[Print[T]] = {
//    import quotes.reflect._
//    println(s"Trying to find print[${Type.show[T]}]")
//    Stack.threadLocalStack.get().get[T] match {
//      case Some(value) => value
//      case _ =>
//        val print: Expr[Print[T]] = Expr.summon[Print[T]] match {
//          case Some(p) => p
//          case _ => report.error(s"Could not find Print for $t"); '
//          {???}
//        }
//        print
//    }
//  }
//}
