package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.Recursive.{Branch, Leaf, Tree}
import com.sksamuel.avro4s.{AvroSchema, Decoder, ImmutableRecord, Recursive}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class CaseClassFourFields(a: String, b: Boolean, c: Int, d: Double)
case class CaseClassOneField(a: String)
case class CaseClassWithNestedCaseClass(a: String, b: CaseClassOneField)

class RecordDecoderBuilderTest extends AnyFunSuite with Matchers {

  test("Decoder.record simple class with four fields") {
    import Decoder.field

    val decoder =
      Decoder.record[CaseClassFourFields](name = "Four", namespace = "test")(
        field[String]("a").map(_.toUpperCase),
        field[Boolean]("b"),
        field[Int]("c"),
        field[Double]("d"))(CaseClassFourFields.apply _)

    val record =
      ImmutableRecord(decoder.schema, Seq[AnyRef]("One", Boolean.box(false), Integer.valueOf(3), Double.box(4)))

    decoder.decode(record) shouldBe CaseClassFourFields("ONE", false, 3, 4.0)
  }

  test("Decoder.record nested class") {
    import Decoder.field

    val decoder =
      Decoder.record[CaseClassWithNestedCaseClass](name = "Nested",
                                                   namespace = "test",
                                                   doc = Some("Extensive Documentation of this class"))(
        field[String]("a"),
        field[CaseClassOneField]("b"))(CaseClassWithNestedCaseClass.apply _)

    val schema = AvroSchema[CaseClassOneField]

    val record =
      ImmutableRecord(decoder.schema, Seq[AnyRef]("One", ImmutableRecord(schema, Seq[AnyRef]("Two"))))

    decoder.decode(record) shouldBe CaseClassWithNestedCaseClass("One", CaseClassOneField("Two"))
  }

  test("Decoder.record with recursive types") {
    import Decoder.field

    implicit def branchDecoder: Decoder[Branch[String]] =
      Decoder.record[Branch[String]](
        name = "StringBranch",
        namespace = "com.sksamuel.avro4s.Recursive"
      )(field[Tree[String]]("left"), field[Tree[String]]("right"))((l: Tree[String], r: Tree[String]) =>
        Branch[String](l, r))

    val decoder = Decoder[Recursive.Tree[String]].resolveDecoder()

    val branch = branchDecoder.schema
    val leaf = AvroSchema[Leaf[String]]

    val tree = ImmutableRecord(
      branch,
      Seq(ImmutableRecord(leaf, Seq("One")),
          ImmutableRecord(branch, Seq(ImmutableRecord(leaf, Seq("Two")), ImmutableRecord(leaf, Seq("Three"))))))

    decoder.decode(tree) shouldBe Branch(Leaf("One"), Branch(Leaf("Two"), Leaf("Three")))
  }

  test("Decoder.record compilation error if fields don't match") {
    """Decoder.record[CaseClassFourFields]("Four", "test")(field[String]("a"),
      |                                                    field[Int]("c"),
      |                                                    field[Boolean]("b"),
      |                                                    field[Double]("d"))(CaseClassFourFields.apply _)
      |""".stripMargin shouldNot compile
  }
}
