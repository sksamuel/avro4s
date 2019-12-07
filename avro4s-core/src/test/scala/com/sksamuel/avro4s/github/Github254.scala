package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class Github254 extends AnyFunSuite with Matchers {

  case class A(c: C)
  case class B(as: List[A])

  sealed trait C
  case object c1 extends C
  case object c2 extends C

  test("NoClassDefFoundError for case class wrapping sealed trait (regression?) #254") {
    AvroSchema[A].toString(true) shouldBe new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github254_a.json")).toString(true)
    AvroSchema[List[A]].toString(true) shouldBe new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github254_lista.json")).toString(true)
    AvroSchema[B].toString(true) shouldBe new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github254_b.json")).toString(true)
  }
}
