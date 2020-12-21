package com.sksamuel.avro4s

import scala.quoted._

class Annotations(quotes: Quotes)(annos: List[quotes.reflect.Term]) {

  import quotes.reflect._

  def doc: Option[String] = annos.flatMap {
    case Apply(fun, args) => fun match {
      case Select(term, name) => term match {
        case New(tree) => tree match {
          // the check here should be done via =:= if possible
          case t: TypeTree if t.tpe.show == classOf[AvroDoc].getName => args.headOption match {
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
}
