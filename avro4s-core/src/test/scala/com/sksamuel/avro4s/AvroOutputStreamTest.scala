
//
//case class WineCrate(wine: Wine)
//case class Test2(dec: BigDecimal)
//
//case class Foo(str: String, boolean: Boolean)
//
//case class NestedListFoo(foos: List[Foo])
//case class NestedListDouble(doubles: List[Double])
//case class NestedListBools(booleans: List[Boolean])
//
//case class NestedSetDoubles(set: Set[Double])
//case class NestedSetTest(set: Set[Foo])
//
//case class NestedSeqDoubles(sequence: Seq[Double])
//case class NestedSeqTest(seq: Seq[Foo])
//
//case class NestedMapTest(map: Map[String, Foo])
//
//case class ValueWrapper(valueClass: ValueClass)
//case class ValueClass(value: String) extends AnyVal
//
//case class EitherCaseClasses(e: Either[WineCrate, Test2])
//

//
//class AvroOutputStreamTest extends WordSpec with Matchers with TimeLimits {
//    "support scala enums" in {
//      val instance = ScalaEnums(Colours.Amber)
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[ScalaEnums](output)
//      avro.write(instance)
//      avro.close()
//
//      val record = read[ScalaEnums](output)
//      record.get("value").toString shouldBe "Amber"
//    }
//    "support LocalDates" in {
//      val instance = LocalDateTest(LocalDate.now())
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[LocalDateTest](output)
//      avro.write(instance)
//      avro.close()
//
//      val record = read[LocalDateTest](output)
//      LocalDate.parse(record.get("localDate").toString) shouldBe instance.localDate
//    }

//    "support bytes" in {
//      case class ByteWrapper(b: Byte)
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[ByteWrapper](output)
//      avro.write(ByteWrapper(3))
//      avro.close()
//
//      val record = read[ByteWrapper](output)
//      record.get("b").asInstanceOf[java.lang.Integer] shouldBe 3
//    }
//    "support Seq[Byte]" in {
//      case class ByteSeq(d: Seq[Byte])
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[ByteSeq](output)
//      avro.write(ByteSeq(Seq[Byte](1, 1, 2, 3, 5, 8)))
//      avro.close()
//
//      val record = read[ByteSeq](output)
//      record.get("d").asInstanceOf[java.nio.ByteBuffer].array().toVector shouldBe Vector[Byte](1, 1, 2, 3, 5, 8)
//    }
//    "support sealed traits with members" in {
//      {
//        val output = new ByteArrayOutputStream
//        val avro = AvroOutputStream.data[Department](output)
//        val sales = Department("sales", BigBoss("Bob"))
//        avro.write(sales)
//        avro.close()
//
//        val record = read[Department](output)
//        record.get("name") shouldBe new Utf8("sales")
//        record.get("head").asInstanceOf[GenericRecord].get("name") shouldBe new Utf8("Bob")
//      }
//      {
//        val output = new ByteArrayOutputStream
//        val avro = AvroOutputStream.data[Department](output)
//        val sales = Department("floor", RankAndFile("Joe", "Foreman"))
//        avro.write(sales)
//        avro.close()
//
//        val record = read[Department](output)
//        record.get("name") shouldBe new Utf8("floor")
//        record.get("head").asInstanceOf[GenericRecord].get("name") shouldBe new Utf8("Joe")
//        record.get("head").asInstanceOf[GenericRecord].get("jobTitle") shouldBe new Utf8("Foreman")
//      }
//    }
//    "support Array[Byte] in Either for data stream" in {
//      val a = EitherWithByte("z", Right("value".getBytes))
//
//      val baos = new ByteArrayOutputStream()
//      val os = AvroOutputStream.data[EitherWithByte](baos)
//      os.write(a)
//      os.close()
//
//      val record = read[EitherWithByte](baos)
//      record.get("key") shouldBe new Utf8("z")
//      record.get("value").asInstanceOf[ByteBuffer].array().toVector shouldBe "value".getBytes.toVector
//    }
//    "support Array[Byte] in Either for a binary stream" in {
//      val a = EitherWithByte("z", Right("value".getBytes))
//
//      val baos = new ByteArrayOutputStream()
//      val os = AvroOutputStream.binary[EitherWithByte](baos)
//      os.write(a)
//      os.close()
//
//      val record = readB[EitherWithByte](baos)
//      record.get("key") shouldBe new Utf8("z")
//      record.get("value").asInstanceOf[ByteBuffer].array().toVector shouldBe "value".getBytes.toVector
//    }
//  }
//}
//
//case class EitherWithByte(key: String, value: Either[Int, Array[Byte]])
//
//object Colours extends Enumeration {
//  val Red, Amber, Green = Value
//}
//case class ScalaEnums(value: Colours.Value)
//
//case class ScalaOptionEnums(value: Option[Colours.Value])