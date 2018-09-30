//  case class Booleans(bool: Boolean)
//  case class BigDecimalTest(decimal: BigDecimal)
//  case class ByteArrayTest(bytes: Array[Byte])
//  case class Floats(float: Float)
//  case class Ints(int: Int)
//  case class Doubles(double: Double)
//  case class Longs(long: Long)
//  case class LongOptions(opt: Option[Long])
//  case class Strings(str: String)
//  case class BooleanOptions(opt: Option[Boolean])
//  case class StringOptions(opt: Option[String])
//  case class SeqCaseClasses(foos: Seq[Foo])
//  case class OptionNestedStrings(opt: Option[Strings])
//  case class SeqStrings(stings: Seq[String])
//  case class SeqDoubles(opt: Seq[Double])
//  case class SeqInts(opt: Seq[Int])
//  case class ArrayStrings(array: Array[String])
//  case class ArrayCaseClasses(array: Array[Foo])
//  case class ArrayDoubls(array: Array[Double])
//  case class ArrayInts(array: Array[Int])
//  case class ListStrings(list: List[String])
//  case class ListCaseClasses(list: List[Foo])
//  case class ListDoubles(list: List[Double])
//  case class ListInts(list: List[Int])
//  case class VectorInts(ints: Vector[Int])
//  case class VectorRecords(records: Vector[Foo])
//  case class SetStrings(set: Set[String])
//  case class SetCaseClasses(set: Set[Foo])
//  case class EitherStringBoolean(either: Either[String, Boolean])
//  case class EitherArray(payload: Either[Seq[Int], String])
//  case class EitherMap(payload: Either[Map[String, Int], Boolean])
//  case class MapBoolean(map: Map[String, Boolean])
//  case class MapSeq(map: Map[String, Seq[String]])
//  case class MapOptions(map: Map[String, Option[String]])
//  case class MapCaseClasses(map: Map[String, Foo])
//  case class MapStrings(map: Map[String, String])
//  case class MapInts(map: Map[String, Int])
//  case class Joo(long: Long)
//  case class Goo(double: Double)
//  case class Moo(either: Either[Joo, Goo])
//  case class Enums(wine: Wine)
//  case class ComplexElement(unit: Ints, decimal: Doubles, flag: Booleans)
//  case class ComplexType(elements: Seq[ComplexElement])
//  case class CoPrimitives(cp: String :+: Boolean :+: CNil)
//  case class CoRecords(cp: Joo :+: Goo :+: CNil)
//  case class CoArrays(cp: Seq[String] :+: Int :+: CNil)
//  case class CoMaps(cp: Map[String, Int] :+: Int :+: CNil)
//

//  "AvroInputStream" should {
//    "read coproducts of primitives" in {
//      type SB = String :+: Boolean :+: CNil
//      val data = Seq(
//        CoPrimitives(Coproduct[SB]("gammy")),
//        CoPrimitives(Coproduct[SB](true)),
//        CoPrimitives(Coproduct[SB](false))
//      )
//      val bytes = write(data)
//
//      val in = AvroInputStream.data[CoPrimitives](bytes)
//      in.iterator.toList shouldBe data.toList
//      in.close()
//    }
//    "read coproducts of case classes" in {
//      type JG = Joo :+: Goo :+: CNil
//      val data = Seq(
//        CoRecords(Coproduct[JG](Joo(98l))),
//        CoRecords(Coproduct[JG](Goo(9.4d)))
//      )
//      val bytes = write(data)
//
//      val in = AvroInputStream.data[CoRecords](bytes)
//      in.iterator.toList shouldBe data.toList
//      in.close()
//    }
//    "read coproducts of arrays" in {
//      type SSI = Seq[String] :+: Int :+: CNil
//      val data = Seq(
//        CoArrays(Coproduct[SSI](Seq("hello", "goodbye"))),
//        CoArrays(Coproduct[SSI](4))
//      )
//
//      val bytes = write(data)
//      val in = AvroInputStream.data[CoArrays](bytes)
//      in.iterator.toList shouldBe data.toList
//      in.close()
//    }
//    "read coproducts of maps" in {
//      type MSII = Map[String, Int] :+: Int :+: CNil
//      val data = Seq(
//        CoMaps(Coproduct[MSII](Map("v" -> 1))),
//        CoMaps(Coproduct[MSII](9))
//      )
//
//      val bytes = write(data)
//      println(bytes)
//      val in = AvroInputStream.data[CoMaps](bytes)
//      in.iterator.toList shouldBe data.toList
//      in.close()
//    }
//  }
//}
