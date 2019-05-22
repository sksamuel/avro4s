package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.{FunSuite, Matchers}

case class Payload[A](value: A)

sealed trait Complex

sealed trait InvertibleComplex extends Complex

case class Foo(value: String, payload: Payload[String]) extends InvertibleComplex

case class Bar(value: Int) extends InvertibleComplex

case class InvertibleComplexWrapper(unwrap: InvertibleComplex)

case class Invert(i: InvertibleComplexWrapper) extends Complex

class Github292 extends FunSuite with Matchers {
  test("Introducing type-parametrised values breaks derivation for some ADTs #292") {
    SchemaFor[Complex].schema().toString(true) shouldBe new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github292.json")).toString(true)
  }
}
