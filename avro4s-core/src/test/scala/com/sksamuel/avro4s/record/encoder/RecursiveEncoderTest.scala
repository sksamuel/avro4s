package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RecursiveEncoderTest extends AnyFunSuite with Matchers {

  test("schema") {
    println(SchemaFor[Tree[Int]].schema.toString(true))
  }

  ignore("recursive encoding") {
    Branch(Branch(Leaf(1), Leaf(2)), Leaf(3))
  }
}

sealed trait Tree[+T]
case class Branch[+T](left: Tree[T], right: Tree[T]) extends Tree[T]
case class Leaf[+T](value: T) extends Tree[T]
