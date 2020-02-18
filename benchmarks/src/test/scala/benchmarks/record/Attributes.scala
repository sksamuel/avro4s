package benchmarks.record

import java.time.Instant

sealed trait IntAttributeValue {
  def timestamp: Instant
}

object IntAttributeValue {
  final case class Empty(timestamp: Instant) extends IntAttributeValue

  final case class Invalid(value: Option[String], timestamp: Instant, errors: Set[String]) extends IntAttributeValue

  final case class Valid(value: Int, timestamp: Instant) extends IntAttributeValue
}

sealed trait AttributeValue[+A] {
  def timestamp: Instant
}

object AttributeValue {

  final case class Empty(timestamp: Instant) extends AttributeValue[Nothing]

  final case class Invalid(value: Option[String], timestamp: Instant, errors: Set[String])
      extends AttributeValue[Nothing]

  final case class Valid[A](value: A, timestamp: Instant) extends AttributeValue[A]

}
