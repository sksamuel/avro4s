package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.Recursive._
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.Coproduct

import scala.collection.JavaConverters._

class RecursiveDecoderTest extends AnyWordSpec with Matchers {

  "decoder" should {
    "support recursive types with sealed traits" in {
      val branch = AvroSchema[Branch[String]]
      val leaf = AvroSchema[Leaf[String]]
      val tree = Branch(Leaf("a"), Branch(Leaf("b"), Leaf("c")))
      val avro = record(branch, record(leaf, "a"), record(branch, record(leaf, "b"), record(leaf, "c")))
      Decoder[Tree[String]].decode(avro) shouldBe tree
    }

    "support mutually recursive types" in {
      val mutRec1 = AvroSchema[MutRec1]
      val mutRec2 = AvroSchema[MutRec2]
      val rec = MutRec1(5, List(MutRec2("five", List(MutRec1(4, List.empty))), MutRec2("four", List.empty)))
      val avro =
        record(mutRec1,
               5,
               list(record(mutRec2, "five", list(record(mutRec1, 4, list()))), record(mutRec2, "four", list())))
      Decoder[MutRec1].decode(avro) shouldBe rec
    }

    "support recursive types with lists" in {
      val listTree = AvroSchema[Recursive.ListTree[Int]]
      val tree = ListTree(1, List(ListTree(2), ListTree(3)))
      val avro = record(listTree, 1, list(record(listTree, 2, list()), record(listTree, 3, list())))
      Decoder[ListTree[Int]].decode(avro) shouldBe tree
    }

    "support recursive types with maps" in {
      val mapTree = AvroSchema[MapTree[Int]]
      val tree = MapTree[Int](1, Map("child" -> MapTree(2, Map("child" -> MapTree(3)))))
      val avro = record(mapTree, 1, map("child" -> record(mapTree, 2, map("child" -> record(mapTree, 3, map())))))
      Decoder[MapTree[Int]].decode(avro) shouldBe tree
    }

    "support recursive types with option" in {
      val optTree = AvroSchema[OptionTree[Int]]
      val tree = OptionTree[Int](1, Some(OptionTree(2, Some(OptionTree(3)))))
      val avro = record(optTree, 1, record(optTree, 2, record(optTree, 3, null, null), null), null)
      Decoder[OptionTree[Int]].decode(avro) shouldBe tree
    }

    "support recursive types with either" in {
      val branch = AvroSchema[EitherBranch[Int]]
      val leaf = AvroSchema[EitherLeaf[Int]]
      val tree: EitherTree[Int] =
        Left(EitherBranch(Right(EitherLeaf(1)), Left(EitherBranch(Right(EitherLeaf(2)), Right(EitherLeaf(3))))))
      val avro = record(branch, record(leaf, 1), record(branch, record(leaf, 2), record(leaf, 3)))
      Decoder[EitherTree[Int]].decode(avro) shouldBe tree
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
      Decoder[CoproductTree[Int]].decode(avro) shouldBe tree
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
      Decoder[TVTree[Int]].decode(avro) shouldBe tree
    }

    "support custom definitions" in {
      // custom decoders for recursive types need to participate in forming a cyclic reference graph of decoders.
      // this requires organizing the code carefully.

      // First important steps:
      // 1. use implicit so that Decoder.apply[Recursive.Tree[Int]] below picks this resolvable decoder for decoding branches.
      // 2. use def so that the recursive expression compiles.
      implicit def branchDecoder: Decoder[Recursive.Branch[Int]] = new ResolvableDecoder[Recursive.Branch[Int]] {

        def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[Branch[Int]] =
          // lookup in the definition environment whether we already have created a decoder for branch.
          env.get[Recursive.Branch[Int]].getOrElse {

            // use var here to first create an acyclic graph and close it later.
            var treeDecoder: Decoder[Recursive.Tree[Int]] = null

            // create a partially initialized decoder for branches (it lacks a value for treeDecoder on creation).
            val decoder = new Decoder[Recursive.Branch[Int]] {
              val schemaFor: SchemaFor[Branch[Int]] = SchemaFor[Branch[Int]]

              def decode(value: Any): Branch[Int] = value match {
                case r: GenericRecord => Branch(treeDecoder.decode(r.get(1)), treeDecoder.decode(r.get(0)))
                case _                => throw new IllegalArgumentException(s"Branch decoder can only decode records, got $value")
              }
            }

            // extend the definition environment with the newly created decoder so that subsequent lookups can return it
            val nextEnv = env.updated(decoder)

            // 1. resolve the tree decoder with the extended environment; the extended env will be passed back to the
            //    lookup performed above.
            // 2. complete the initialization by closing the reference cycle: the branch decoder and tree decoder now
            //    mutually reference each other.
            treeDecoder = Decoder.apply[Recursive.Tree[Int]].resolveDecoder(nextEnv, update)
            decoder
          }
      }

      // summon decoder for tree and kick off decoder resolution.
      val decoder = Decoder[Recursive.Tree[Int]].resolveDecoder()

      val tree = Branch(Leaf(1), Branch(Leaf(2), Leaf(3)))

      val branch = AvroSchema[Branch[Int]]
      val leaf = AvroSchema[Leaf[Int]]
      val avro = record(branch, record(branch, record(leaf, 3), record(leaf, 2)), record(leaf, 1))

      // use the resolved decoder.
      decoder.decode(avro) shouldBe tree
    }

    def record(schema: Schema, values: Any*): ImmutableRecord = ImmutableRecord(schema, values.map(asAvro))

    def list(values: Any*) = values.map(asAvro).asJava

    def map(keyvalues: (String, Any)*) = keyvalues.toMap.map(kv => kv._1 -> asAvro(kv._2)).asJava

    def asAvro(value: Any): AnyRef = value match {
      case s: String => new Utf8(s)
      case i: Int    => Integer.valueOf(i)
      case o: AnyRef => o
      case null      => null
    }
  }
}
