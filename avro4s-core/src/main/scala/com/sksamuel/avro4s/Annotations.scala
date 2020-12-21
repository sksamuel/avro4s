package com.sksamuel.avro4s

import scala.quoted._

class Annotations(quotes: Quotes)(annos: List[quotes.reflect.Term]) {

  import quotes.reflect._

  def isPresent(clazz: Class[_]): Boolean = annos.exists {
    case Apply(fun, args) => fun match {
      case Select(term, name) => term match {
        case New(tree) => tree match {
          // the check here should be done via =:= if possible
          case t: TypeTree if t.tpe.show == clazz.getName => true
          case _ => false
        }
        case _ => false
      }
      case _ => false
    }
    case _ => false
  }

  /**
   * Returns the string constant value from the first annotation that matches the given class.
   */
  def findStringValue(clazz: Class[_]): Option[String] = annos.flatMap {
    case Apply(fun, args) => fun match {
      case Select(term, name) => term match {
        case New(tree) => tree match {
          // the check here should be done via =:= if possible
          case t: TypeTree if t.tpe.show == clazz.getName => args.headOption match {
            case Some(Literal(const: Constant)) => Some(const.value.toString())
            case _ => None
          }
          case _ => None
        }
        case _ => None
      }
      case _ => None
    }
    case _ => None
  }.headOption

  def error: Boolean = isPresent(classOf[AvroError])
  def doc: Option[String] = findStringValue(classOf[AvroDoc])
  def namespace: Option[String] = findStringValue(classOf[AvroNamespace])
}
