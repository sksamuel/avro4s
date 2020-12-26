package com.sksamuel.avro4s.encoders

import scala.deriving.Mirror

trait LowPriorityEncoders {
  /**
   * Creates an [[Encoder]] for T by using a macro derived implementation.
   */
  inline given derive[T](using m: Mirror.Of[T]): Encoder[T] = MacroEncoder.derive[T]
}
