package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream
import java.time.LocalDate

import org.scalatest.concurrent.TimeLimits
import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil, Coproduct}

class AvroInputStreamTest extends WordSpec with Matchers with TimeLimits {

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
  case class VectorInts(ints: Vector[Int])
  case class VectorRecords(records: Vector[Foo])
  case class SetStrings(set: Set[String])
  case class SetCaseClasses(set: Set[Foo])
  case class EitherStringBoolean(either: Either[String, Boolean])
  case class EitherArray(payload: Either[Seq[Int], String])
  case class EitherMap(payload: Either[Map[String, Int], Boolean])
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
  case class ComplexElement(unit: Ints, decimal: Doubles, flag: Booleans)
  case class ComplexType(elements: Seq[ComplexElement])
  case class CoPrimitives(cp: String :+: Boolean :+: CNil)
  case class CoRecords(cp: Joo :+: Goo :+: CNil)
  case class CoArrays(cp: Seq[String] :+: Int :+: CNil)
  case class CoMaps(cp: Map[String, Int] :+: Int :+: CNil)

  def write[T](ts: Seq[T])(implicit schema: SchemaFor[T], ser: ToRecord[T]): Array[Byte] = {
    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[T](output)
    avro.write(ts)
    avro.close()
    output.toByteArray
  }

  "AvroInputStream" should {
    "read big decimals" in {
      val data = Seq(BigDecimalTest(1235.52), BigDecimalTest(1234.68))
      val bytes = write(data)

      val in = AvroInputStream.data[BigDecimalTest](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read complex type" in {
      val data = Seq(ComplexType(Seq(ComplexElement(Ints(2), Doubles(0.12345), Booleans(true)), ComplexElement(Ints(5), Doubles(0.98568), Booleans(false)))))
      val bytes = write(data)

      val in = AvroInputStream.data[ComplexType](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read byte arrays" in {
      val data = Seq(ByteArrayTest(Array[Byte](1, 2, 3)), ByteArrayTest(Array[Byte](125, 126, 127)))
      val bytes = write(data)

      val in = AvroInputStream.data[ByteArrayTest](bytes)
      val result = in.iterator.toList
      result.head.bytes.toList shouldBe Array[Byte](1, 2, 3).toList
      result.last.bytes.toList shouldBe Array[Byte](125, 126, 127).toList
      in.close()
    }
    "read eithers of nested case classes" in {
      val data = Seq(Moo(Left(Joo(4l))), Moo(Right(Goo(12.5d))), Moo(Right(Goo(3))))
      val bytes = write(data)

      val in = AvroInputStream.data[Moo](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read eithers of primitives" in {
      val data = Seq(EitherStringBoolean(Left("sammy")), EitherStringBoolean(Right(true)), EitherStringBoolean(Right(false)))
      val bytes = write(data)

      val in = AvroInputStream.data[EitherStringBoolean](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read eithers of arrays" in {
      val data = Seq(EitherArray(Left(Seq(1,2))), EitherArray(Right("lammy")))

      val bytes = write(data)
      val in = AvroInputStream.data[EitherArray](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read eithers of maps" in {
      val data = Seq(EitherMap(Left(Map("val" -> 4))), EitherMap(Right(true)))

      val bytes = write(data)

      val in = AvroInputStream.data[EitherMap](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read coproducts of primitives" in {
      type SB = String :+: Boolean :+: CNil
      val data = Seq(
        CoPrimitives(Coproduct[SB]("gammy")),
        CoPrimitives(Coproduct[SB](true)),
        CoPrimitives(Coproduct[SB](false))
      )
      val bytes = write(data)

      val in = AvroInputStream.data[CoPrimitives](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read coproducts of case classes" in {
      type JG = Joo :+: Goo :+: CNil
      val data = Seq(
        CoRecords(Coproduct[JG](Joo(98l))),
        CoRecords(Coproduct[JG](Goo(9.4d)))
      )
      val bytes = write(data)

      val in = AvroInputStream.data[CoRecords](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read coproducts of arrays" in {
      type SSI = Seq[String] :+: Int :+: CNil
      val data = Seq(
        CoArrays(Coproduct[SSI](Seq("hello", "goodbye"))),
        CoArrays(Coproduct[SSI](4))
      )

      val bytes = write(data)
      val in = AvroInputStream.data[CoArrays](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read coproducts of maps" in {
      type MSII = Map[String, Int] :+: Int :+: CNil
      val data = Seq(
        CoMaps(Coproduct[MSII](Map("v" -> 1))),
        CoMaps(Coproduct[MSII](9))
      )

      val bytes = write(data)
      println(bytes)
      val in = AvroInputStream.data[CoMaps](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of booleans" in {
      val data = Seq(MapBoolean(Map("sammy" -> true, "hammy" -> false)))
      val bytes = write(data)

      val in = AvroInputStream.data[MapBoolean](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of seqs of strings" in {
      val data = Seq(MapSeq(Map("sammy" -> Seq("foo", "moo"), "hammy" -> Seq("boo", "goo"))))
      val bytes = write(data)

      val in = AvroInputStream.data[MapSeq](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of options" in {
      val data = Seq(MapOptions(Map("sammy" -> None, "hammy" -> Some("foo"))))
      val bytes = write(data)

      val in = AvroInputStream.data[MapOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of case classes" in {
      val data = Seq(MapCaseClasses(Map("sammy" -> Foo("sam", true), "hammy" -> Foo("ham", false))))
      val bytes = write(data)

      val in = AvroInputStream.data[MapCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read deep nested maps" in {
      val data = Level1(Level2(Level3(Level4(Map("a" -> "b")))))
      val bytes = write(Seq(data))
      val in = AvroInputStream.data[Level1](bytes)
      in.iterator.toList shouldBe List(data)
      in.close()
    }
    "read maps of strings" in {
      val data = Seq(MapStrings(Map("sammy" -> "foo", "hammy" -> "boo")))
      val bytes = write(data)

      val in = AvroInputStream.data[MapStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of ints" in {

      val data = Seq(MapInts(Map("sammy" -> 1, "hammy" -> 2)))
      val bytes = write(data)

      val in = AvroInputStream.data[MapInts](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read set of strings" in {
      val data = Seq(SetStrings(Set("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream.data[SetStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read set of case classes" in {
      val data = Seq(SetCaseClasses(Set(Foo("sammy", true), Foo("hammy", false))))
      val bytes = write(data)

      val in = AvroInputStream.data[SetCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of ints" in {

      val data = Seq(ListInts(List(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream.data[ListInts](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of doubles" in {

      val data = Seq(ListDoubles(List(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream.data[ListDoubles](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of strings" in {


      val data = Seq(ListStrings(List("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream.data[ListStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of case classes" in {

      val data = Seq(ListCaseClasses(List(Foo("sammy", true), Foo("hammy", false))))
      val bytes = write(data)

      val in = AvroInputStream.data[ListCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read array of ints" in {

      val data = Seq(ArrayInts(Array(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream.data[ArrayInts](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of doubles" in {

      val data = Seq(ArrayDoubls(Array(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream.data[ArrayDoubls](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of strings" in {
      val data = Seq(ArrayStrings(Array("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream.data[ArrayStrings](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of case classes" in {

      val data = Seq(ArrayCaseClasses(Array(Foo("sammy", true), Foo("hammy", false))))
      val bytes = write(data)

      val in = AvroInputStream.data[ArrayCaseClasses](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read seq of ints" in {

      val data = Seq(SeqInts(Seq(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream.data[SeqInts](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of doubles" in {
      val data = Seq(SeqDoubles(Seq(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream.data[SeqDoubles](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of strings" in {
      val data = Seq(SeqStrings(Seq("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream.data[SeqStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of case classes" in {
      val data = Seq(SeqCaseClasses(Seq(Foo("sammy", false), Foo("hammy", true))))
      val bytes = write(data)

      val in = AvroInputStream.data[SeqCaseClasses](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of case classes" in {

      val data = Seq(OptionNestedStrings(Option(Strings("sammy"))), OptionNestedStrings(None))
      val bytes = write(data)

      val in = AvroInputStream.data[OptionNestedStrings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of strings" in {
      val data = Seq(StringOptions(Option("sammy")), StringOptions(None))
      val bytes = write(data)

      val in = AvroInputStream.data[StringOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of booleans" in {
      val data = Seq(BooleanOptions(Option(true)), BooleanOptions(None))
      val bytes = write(data)

      val in = AvroInputStream.data[BooleanOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of longs" in {
      val data = Seq(LongOptions(Option(4)), LongOptions(None))
      val bytes = write(data)

      val in = AvroInputStream.data[LongOptions](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read strings" in {

      val data = Seq(Strings("sammy"), Strings("hammy"))
      val bytes = write(data)

      val in = AvroInputStream.data[Strings](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read booleans" in {
      val data = Seq(Booleans(true), Booleans(false))
      val bytes = write(data)
      val in = AvroInputStream.data[Booleans](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read doubles" in {

      val data = Seq(Doubles(1.2d), Doubles(2.3d))
      val bytes = write(data)

      val in = AvroInputStream.data[Doubles](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read floats" in {
      val data = Seq(Floats(1.2f), Floats(3.4f))
      val bytes = write(data)

      val in = AvroInputStream.data[Floats](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read ints" in {
      val data = Seq(Ints(1), Ints(2))
      val bytes = write(data)

      val in = AvroInputStream.data[Ints](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read longs" in {

      val data = Seq(Longs(1l), Longs(2l))
      val bytes = write(data)

      val in = AvroInputStream.data[Longs](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read scala enums" in {

      val data = Seq(ScalaEnums(Colours.Red), ScalaEnums(Colours.Green))
      val bytes = write(data)

      val in = AvroInputStream.data[ScalaEnums](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read LocalDates" in {

      val data = Seq(LocalDateTest(LocalDate.now()), LocalDateTest(LocalDate.now()))
      val bytes = write(data)

      val in = AvroInputStream.data[LocalDateTest](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "support value classes" in {
      val data = Seq(ValueWrapper(ValueClass("bob")), ValueWrapper(ValueClass("ann")))
      val bytes = write(data)

      val in = AvroInputStream.data[ValueWrapper](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read vectors of ints" in {
      val data = Seq(VectorInts(Vector(3, 2, 1)))
      val bytes = write(data)

      val in = AvroInputStream.data[VectorInts](bytes)
      in.iterator.toList shouldBe Seq(VectorInts(Vector(3, 2, 1)))
      in.close()
    }
    "read vectors of records" in {
      val data = Seq(VectorRecords(Vector(Foo("sammy", true), Foo("hammy", false))))
      val bytes = write(data)

      val in = AvroInputStream.data[VectorRecords](bytes)
      in.iterator.toList shouldBe Seq(VectorRecords(Vector(Foo("sammy", true), Foo("hammy", false))))
      in.close()
    }
  }
}
