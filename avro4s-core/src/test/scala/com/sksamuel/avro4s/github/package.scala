package com.sksamuel.avro4s

package object github {

  sealed trait Moo
  case object Moo1 extends Moo
  case object Moo2 extends Moo

  case class MooWrapper(moo: Moo)
}
