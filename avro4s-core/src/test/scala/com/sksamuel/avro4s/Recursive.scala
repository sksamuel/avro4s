package com.sksamuel.avro4s

import shapeless.{:+:, CNil}

object Recursive {

  sealed trait Tree[+T]
  case class Branch[+T](left: Tree[T], right: Tree[T]) extends Tree[T]
  case class Leaf[+T](value: T) extends Tree[T]

  case class MutRec1(payload: Int, children: List[MutRec2])
  case class MutRec2(payload: String, children: List[MutRec1])

  case class ListTree[+T](value: T, children: Seq[ListTree[T]] = Seq.empty)
  case class MapTree[+T](value: T, children: Map[String, MapTree[T]] = Map.empty[String, MapTree[T]])
  case class OptionTree[T](value: T, left: Option[OptionTree[T]] = None, right: Option[OptionTree[T]] = None)

  type EitherTree[T] = Either[EitherBranch[T], EitherLeaf[T]]
  case class EitherLeaf[T](value: T)
  case class EitherBranch[T](left: EitherTree[T], right: EitherTree[T])

  type CoproductTree[T] = CBranch[T] :+: CLeaf[T] :+: CNil
  case class CLeaf[T](value: T)
  case class CBranch[T](left: CoproductTree[T], right: CoproductTree[T])

  sealed trait TVTree[+T]
  case class TreeValue[T](tree: TVTree[T]) extends AnyVal
  case class TVBranch[T](left: (Int, TreeValue[T]), right: (Int, TreeValue[T])) extends TVTree[T]
  case class TVLeaf[T](value: T) extends TVTree[T]
}