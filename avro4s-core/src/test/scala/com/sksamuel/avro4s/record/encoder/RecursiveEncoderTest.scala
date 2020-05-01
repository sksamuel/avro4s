package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.Recursive._
import com.sksamuel.avro4s.{Encoder, _}
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.Coproduct
import scala.collection.JavaConverters._

class RecursiveEncoderTest extends AnyWordSpec with Matchers {

  "encoder" should {
    "support recursive types with sealed traits" in {
      val branch = AvroSchema[Branch[String]]
      val leaf = AvroSchema[Leaf[String]]
      val tree = Branch(Leaf("a"), Branch(Leaf("b"), Leaf("c")))
      val avro = record(branch, record(leaf, "a"), record(branch, record(leaf, "b"), record(leaf, "c")))
      Encoder[Tree[String]].encode(tree) shouldBe avro
    }

    "support mutually recursive types" in {
      val mutRec1 = AvroSchema[MutRec1]
      val mutRec2 = AvroSchema[MutRec2]
      val rec = MutRec1(5, List(MutRec2("five", List(MutRec1(4, List.empty))), MutRec2("four", List.empty)))
      val avro =
        record(mutRec1,
               5,
               list(record(mutRec2, "five", list(record(mutRec1, 4, list()))), record(mutRec2, "four", list())))
      Encoder[MutRec1].encode(rec) === avro shouldBe true
    }

    "support recursive types with lists" in {
      val listTree = AvroSchema[Recursive.ListTree[Int]]
      val tree = ListTree(1, List(ListTree(2), ListTree(3)))
      val avro = record(listTree, 1, list(record(listTree, 2, list()), record(listTree, 3, list())))
      Encoder[ListTree[Int]].encode(tree) shouldBe avro
    }

    "support recursive types with maps" in {
      val mapTree = AvroSchema[MapTree[Int]]
      val tree = MapTree[Int](1, Map("child" -> MapTree(2, Map("child" -> MapTree(3)))))
      val avro = record(mapTree, 1, map("child" -> record(mapTree, 2, map("child" -> record(mapTree, 3, map())))))
      Encoder[MapTree[Int]].encode(tree) shouldBe avro
    }

    "support recursive types with option" in {
      val optTree = AvroSchema[OptionTree[Int]]
      val tree = OptionTree[Int](1, Some(OptionTree(2, Some(OptionTree(3)))))
      val avro = record(optTree, 1, record(optTree, 2, record(optTree, 3, null, null), null), null)
      Encoder[OptionTree[Int]].encode(tree) shouldBe avro
    }

    "support recursive types with either" in {
      val branch = AvroSchema[EitherBranch[Int]]
      val leaf = AvroSchema[EitherLeaf[Int]]
      val tree: EitherTree[Int] =
        Left(EitherBranch(Right(EitherLeaf(1)), Left(EitherBranch(Right(EitherLeaf(2)), Right(EitherLeaf(3))))))
      val avro = record(branch, record(leaf, 1), record(branch, record(leaf, 2), record(leaf, 3)))
      Encoder[EitherTree[Int]].encode(tree) shouldBe avro
    }

    "support recursive types with shapeless coproduct" in {
      val branch = AvroSchema[CBranch[Int]]
      val leaf = AvroSchema[CLeaf[Int]]
      type Tree = CoproductTree[Int]
      val tree: Tree =
        Coproduct[Tree](
          CBranch(Coproduct[Tree](CLeaf(1)),
                  Coproduct[Tree](CBranch(Coproduct[Tree](CLeaf(2)), Coproduct[Tree](CLeaf(3))))))
      val avro = record(branch, record(leaf, 1), record(branch, record(leaf, 2), record(leaf, 3)))
      Encoder[CoproductTree[Int]].encode(tree) shouldBe avro
    }

    "support recursive types with tuples and value types" in {
      val branch = AvroSchema[TVBranch[Int]]
      val leaf = AvroSchema[TVLeaf[Int]]
      val pair = AvroSchema[(Int, TreeValue[Int])]

      val tree: TVTree[Int] = TVBranch(1 -> TreeValue(TVLeaf(1)),
                                       2 -> TreeValue(TVBranch(3 -> TreeValue(TVLeaf(2)), 4 -> TreeValue(TVLeaf(3)))))
      val avro =
        record(branch,
               record(pair, 1, record(leaf, 1)),
               record(pair, 2, record(branch, record(pair, 3, record(leaf, 2)), record(pair, 4, record(leaf, 3)))))
      Encoder[TVTree[Int]].encode(tree) shouldBe avro
    }

  def record(schema: Schema, values: Any*): ImmutableRecord = ImmutableRecord(schema, values.map(asAvro))

  def list(values: Any*) = values.map(asAvro).asJava

  def map(keyvalues: (String, Any)*) = keyvalues.toMap.mapValues(asAvro).asJava

  def asAvro(value: Any): AnyRef = value match {
    case s: String => new Utf8(s)
    case i: Int    => Integer.valueOf(i)
    case o: AnyRef => o
    case null      => null
  }
}}
