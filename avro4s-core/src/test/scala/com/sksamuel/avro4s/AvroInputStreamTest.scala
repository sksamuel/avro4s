package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream
import java.util.UUID

import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroInputStreamTest extends WordSpec with Matchers with Timeouts {

  case class Booleans(bool: Boolean)
  case class BigDecimalTest(decimal: BigDecimal)
  case class ByteArrayTest(bytes: Array[Byte])
  case class Floats(float: Float)
  case class Ints(int: Int)
  case class Doubles(double: Double)
  case class Longs(long: Long)
  case class LongOptions(opt: Option[Long])
  case class Strings(str: String)
  case class BooleanOptions(opt: Option[Boolean])
  case class StringOptions(opt: Option[String])
  case class SeqCaseClasses(foos: Seq[Foo])
  case class OptionNestedStrings(opt: Option[Strings])
  case class SeqStrings(stings: Seq[String])
  case class SeqDoubles(opt: Seq[Double])
  case class SeqInts(opt: Seq[Int])
  case class ArrayStrings(array: Array[String])
  case class ArrayCaseClasses(array: Array[Foo])
  case class ArrayDoubls(array: Array[Double])
  case class ArrayInts(array: Array[Int])
  case class ListStrings(list: List[String])
  case class ListCaseClasses(list: List[Foo])
  case class ListDoubles(list: List[Double])
  case class ListInts(list: List[Int])
  case class SetStrings(set: Set[String])
  case class SetCaseClasses(set: Set[Foo])
  case class EitherStringBoolean(either: Either[String, Boolean])
  case class MapBoolean(map: Map[String, Boolean])
  case class MapSeq(map: Map[String, Seq[String]])
  case class MapOptions(map: Map[String, Option[String]])
  case class MapCaseClasses(map: Map[String, Foo])
  case class MapStrings(map: Map[String, String])
  case class MapInts(map: Map[String, Int])
  case class Joo(long: Long)
  case class Goo(double: Double)
  case class Moo(either: Either[Joo, Goo])
  case class Enums(wine: Wine)


  def write[T](ts: Seq[T])(implicit schema: SchemaFor[T], ser: ToRecord[T]): Array[Byte] = {
    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream[T](output)
    avro.write(ts)
    avro.close()
    output.toByteArray
  }

  "AvroInputStream" should {
    "read enums" in {
      val data = Seq(Enums(Wine.Malbec), Enums(Wine.Shiraz))
      val bytes = write(data)

      val in = AvroInputStream[Enums](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read big decimals" in {
      val data = Seq(BigDecimalTest(1235.52344), BigDecimalTest(1234))
      val bytes = write(data)

      val in = AvroInputStream[BigDecimalTest](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read byte arrays" in {
      val data = Seq(ByteArrayTest(Array[Byte](1, 2, 3)), ByteArrayTest(Array[Byte](125, 126, 127)))
      val bytes = write(data)

      val in = AvroInputStream[ByteArrayTest](bytes)
      val result = in.iterator.toList
      result.head.bytes.toList shouldBe Array[Byte](1, 2, 3).toList
      result.last.bytes.toList shouldBe Array[Byte](125, 126, 127).toList
      in.close()
    }
    "read eithers of nested case classes" in {
      val data = Seq(Moo(Left(Joo(4l))), Moo(Right(Goo(12.5d))), Moo(Right(Goo(3))))
      val bytes = write(data)

      val in = AvroInputStream[Moo](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read eithers of primitives" in {
      val data = Seq(EitherStringBoolean(Left("sammy")), EitherStringBoolean(Right(true)), EitherStringBoolean(Right(false)))
      val bytes = write(data)

      val in = AvroInputStream[EitherStringBoolean](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of booleans" in {
      val data = Seq(MapBoolean(Map("sammy" -> true, "hammy" -> false)))
      val bytes = write(data)

      val in = AvroInputStream[MapBoolean](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of seqs of strings" in {
      val data = Seq(MapSeq(Map("sammy" -> Seq("foo", "moo"), "hammy" -> Seq("boo", "goo"))))
      val bytes = write(data)

      val in = AvroInputStream[MapSeq](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of options" in {
      val data = Seq(MapOptions(Map("sammy" -> None, "hammy" -> Some("foo"))))
      val bytes = write(data)

      val in = AvroInputStream[MapOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of case classes" in {
      val data = Seq(MapCaseClasses(Map("sammy" -> Foo("sam", true), "hammy" -> Foo("ham", false))))
      val bytes = write(data)

      val in = AvroInputStream[MapCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read deep nested maps" in {
      val data = Level1(Level2(Level3(Level4(Map("a" -> "b")))))
      val bytes = write(Seq(data))
      val in = AvroInputStream[Level1](bytes)
      in.iterator.toList shouldBe List(data)
      in.close()
    }
    "read maps of strings" in {
      val data = Seq(MapStrings(Map("sammy" -> "foo", "hammy" -> "boo")))
      val bytes = write(data)

      val in = AvroInputStream[MapStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of ints" in {

      val data = Seq(MapInts(Map("sammy" -> 1, "hammy" -> 2)))
      val bytes = write(data)

      val in = AvroInputStream[MapInts](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read set of strings" in {
      val data = Seq(SetStrings(Set("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream[SetStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read set of case classes" in {
      val data = Seq(SetCaseClasses(Set(Foo("sammy", true), Foo("hammy", false))))
      val bytes = write(data)

      val in = AvroInputStream[SetCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of ints" in {

      val data = Seq(ListInts(List(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream[ListInts](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of doubles" in {

      val data = Seq(ListDoubles(List(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream[ListDoubles](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of strings" in {


      val data = Seq(ListStrings(List("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream[ListStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of case classes" in {

      val data = Seq(ListCaseClasses(List(Foo("sammy", true), Foo("hammy", false))))
      val bytes = write(data)

      val in = AvroInputStream[ListCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read array of ints" in {

      val data = Seq(ArrayInts(Array(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream[ArrayInts](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of doubles" in {

      val data = Seq(ArrayDoubls(Array(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream[ArrayDoubls](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of strings" in {
      val data = Seq(ArrayStrings(Array("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream[ArrayStrings](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of case classes" in {

      val data = Seq(ArrayCaseClasses(Array(Foo("sammy", true), Foo("hammy", false))))
      val bytes = write(data)

      val in = AvroInputStream[ArrayCaseClasses](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read seq of ints" in {

      val data = Seq(SeqInts(Seq(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream[SeqInts](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of doubles" in {
      val data = Seq(SeqDoubles(Seq(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream[SeqDoubles](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of strings" in {
      val data = Seq(SeqStrings(Seq("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream[SeqStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of case classes" in {
      val data = Seq(SeqCaseClasses(Seq(Foo("sammy", false), Foo("hammy", true))))
      val bytes = write(data)

      val in = AvroInputStream[SeqCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of case classes" in {

      val data = Seq(OptionNestedStrings(Option(Strings("sammy"))), OptionNestedStrings(None))
      val bytes = write(data)

      val in = AvroInputStream[OptionNestedStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of strings" in {
      val data = Seq(StringOptions(Option("sammy")), StringOptions(None))
      val bytes = write(data)

      val in = AvroInputStream[StringOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of booleans" in {
      val data = Seq(BooleanOptions(Option(true)), BooleanOptions(None))
      val bytes = write(data)

      val in = AvroInputStream[BooleanOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of longs" in {
      val data = Seq(LongOptions(Option(4)), LongOptions(None))
      val bytes = write(data)

      val in = AvroInputStream[LongOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read strings" in {

      val data = Seq(Strings("sammy"), Strings("hammy"))
      val bytes = write(data)

      val in = AvroInputStream[Strings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read booleans" in {
      val data = Seq(Booleans(true), Booleans(false))
      val bytes = write(data)
      val in = AvroInputStream[Booleans](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read doubles" in {

      val data = Seq(Doubles(1.2d), Doubles(2.3d))
      val bytes = write(data)

      val in = AvroInputStream[Doubles](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read floats" in {
      val data = Seq(Floats(1.2f), Floats(3.4f))
      val bytes = write(data)

      val in = AvroInputStream[Floats](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read ints" in {
      val data = Seq(Ints(1), Ints(2))
      val bytes = write(data)

      val in = AvroInputStream[Ints](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read longs" in {

      val data = Seq(Longs(1l), Longs(2l))
      val bytes = write(data)

      val in = AvroInputStream[Longs](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read scala enums" in {

      val data = Seq(ScalaEnums(Colours.Red), ScalaEnums(Colours.Green))
      val bytes = write(data)

      val in = AvroInputStream[ScalaEnums](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read uuids" in {

      val data = Seq(Ids(UUID.randomUUID()), Ids(UUID.randomUUID()))
      val bytes = write(data)

      val in = AvroInputStream[Ids](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
  }
}