package com.sksamuel.avro4s.record.encoder

import org.scalatest.{FunSuite, Matchers}

class RecursiveEncoderTest extends FunSuite with Matchers {

  ignore("recursive encoding") {
    Branch(Branch(Leaf(1), Leaf(2)), Leaf(3))
  }
}

sealed trait Tree[+T]
case class Branch[+T](left: Tree[T], right: Tree[T]) extends Tree[T]
case class Leaf[+T](value: T) extends Tree[T]