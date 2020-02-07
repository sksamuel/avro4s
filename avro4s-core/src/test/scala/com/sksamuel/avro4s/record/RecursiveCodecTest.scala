package com.sksamuel.avro4s.record

import com.sksamuel.avro4s.RecordFormat
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RecursiveCodecTest extends AnyFunSuite with Matchers {

  test("recursive roundtrip") {
    val t = Recursive(
      0,
      List(Recursive(2, List(Recursive(2, Nil))), Recursive(3, Nil))
    )
    val format = RecordFormat[Recursive]
    val rec = format.to(t)
    val t2 = format.from(rec)
    t2 shouldBe t
  }

  test("recursive AST roundtrip") {
    val t = Branch(Branch(Leaf(1), Leaf(2)), Leaf(3))
    val format = RecordFormat[Tree[Int]]
    val rec = format.to(t)
    val t2 = format.from(rec)
    t2 shouldBe t
  }

  test("mutually recursive roundtrip") {
    val t = MutRec1(1, MutRec2("a", Some(MutRec1(2, MutRec2("b", None)))))
    val format = RecordFormat[MutRec1]
    val rec = format.to(t)
    val t2 = format.from(rec)
    t2 shouldBe t
  }
}

sealed trait Tree[+T]
case class Branch[+T](left: Tree[T], right: Tree[T]) extends Tree[T]
case class Leaf[+T](value: T) extends Tree[T]

case class Recursive(payload: Int, list: List[Recursive])

case class MutRec1(payload: Int, mut: MutRec2)
case class MutRec2(payload: String, mut: Option[MutRec1])
