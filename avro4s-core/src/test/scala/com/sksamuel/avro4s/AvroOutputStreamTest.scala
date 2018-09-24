
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
//case class CPWrapper(u: Option[CPWrapper.ISTTB])
//object CPWrapper {
//  type ISTTB = Int :+: String :+: WineCrate :+: Test2 :+: Boolean :+: CNil
//}
//
//class AvroOutputStreamTest extends WordSpec with Matchers with TimeLimits {
//
//    "write a Some as populated union with BigDecimal logical type" in {
//      case class Test(opt: Option[BigDecimal])
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test(Some(123.45)))
//      avro.close()
//
//      val record = read[Test](output)
//      val buffer = record.get("opt").asInstanceOf[ByteBuffer]
//      val bytes = Array.ofDim[Byte](buffer.remaining())
//      buffer.get(bytes)
//      BigDecimal(BigInt(bytes), 2) shouldBe BigDecimal(123.45)
//    }
//    "write a Some as populated union with BigDecimal logical type with default value" in {
//      case class Test(opt: Option[BigDecimal]= Some(1234.56))
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test())
//      avro.close()
//
//      val record = read[Test](output)
//      val buffer = record.get("opt").asInstanceOf[ByteBuffer]
//      val bytes = Array.ofDim[Byte](buffer.remaining())
//      buffer.get(bytes)
//      BigDecimal(BigInt(bytes), 2) shouldBe BigDecimal(1234.56)
//    }
//    "write a Some as populated union" in {
//      case class Test(opt: Option[Double])
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test(Some(123.456d)))
//      avro.close()
//
//      val record = read[Test](output)
//      record.get("opt").toString shouldBe "123.456"
//    }
//    "write a None as union null" in {
//      case class Test(opt: Option[Double])
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test(None))
//      avro.close()
//
//      val record = read[Test](output)
//      record.get("opt") shouldBe null
//    }
//    "write out primitives in coproducts as unions" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[CPWrapper](output)
//      avro.write(CPWrapper(Some(Coproduct[CPWrapper.ISTTB](4))))
//      avro.close()
//
//      val record = read[CPWrapper](output)
//      record.get("u").toString shouldBe "4"
//    }
//    "write out classes in coproducts as unions" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[CPWrapper](output)
//      avro.write(CPWrapper(Some(Coproduct[CPWrapper.ISTTB](Test2(34.98)))))
//      avro.close()
//
//      val record = read[CPWrapper](output)
//      record.get("u").toString shouldBe """{"dec": {"bytes": """" + """\""" + """rª"}}"""
//    }
//    "write out Nones in coproducts as nulls" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[CPWrapper](output)
//      avro.write(CPWrapper(None))
//      avro.close()
//
//      val record = read[CPWrapper](output)
//      record.get("u") shouldBe null
//    }
//    "" in {
//
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test(Array(1d, 2d, 3d, 4d)))
//      avro.close()
//
//      val record = read[Test](output)
//      record.get("array").asInstanceOf[java.util.List[Double]].asScala shouldBe Seq(1d, 2d, 3d, 4d)
//    }
//    "" in {
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedSeqDoubles](output)
//      avro.write(NestedSeqDoubles(Seq(1d, 2d, 3d, 4d)))
//      avro.close()
//
//      val record = read[NestedSeqDoubles](output)
//      record.get("sequence").asInstanceOf[java.util.List[Double]].asScala shouldBe Seq(1d, 2d, 3d, 4d)
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Level1](output)
//      avro.write(Level1(Level2(Level3(Level4(Map("a" -> "b"))))))
//      avro.close()
//
//      val record = read[Level1](output)
//      record.toString shouldBe """{"level2": {"level3": {"level4": {"str": {"a": "b"}}}}}"""
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedSeqTest](output)
//      avro.write(NestedSeqTest(List(Foo("sam", true), Foo("ham", false))))
//      avro.close()
//
//      val record = read[NestedSeqTest](output)
//      val data = record.get("seq").asInstanceOf[java.util.List[GenericRecord]].asScala.toList
//      data.head.get("str").toString shouldBe "sam"
//      data.last.get("str").toString shouldBe "ham"
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedSetDoubles](output)
//      avro.write(NestedSetDoubles(Set(1d, 9d, 9d, 9d, 9d)))
//      avro.close()
//
//      val record = read[NestedSetDoubles](output)
//      record.get("set").asInstanceOf[java.util.List[Double]].asScala.toSet shouldBe Set(1d, 9d)
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedSetTest](output)
//      avro.write(NestedSetTest(Set(Foo("sam", true), Foo("ham", false))))
//      avro.close()
//
//      val record = read[NestedSetTest](output)
//      val actual = record.get("set").asInstanceOf[java.util.List[GenericRecord]].asScala.toSet
//      actual.map(_.get("str").toString) shouldBe Set("sam", "ham")
//      actual.map(_.get("boolean").toString.toBoolean) shouldBe Set(true, false)
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedListDouble](output)
//      avro.write(NestedListDouble(List(1d, 2d, 3d, 4d)))
//      avro.close()
//
//      val record = read[NestedListDouble](output)
//      record.get("doubles").asInstanceOf[java.util.List[Double]].asScala shouldBe List(1d, 2d, 3d, 4d)
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedListBools](output)
//      avro.write(NestedListBools(List(true, false, true)))
//      avro.close()
//
//      val record = read[NestedListBools](output)
//      record.get("booleans").asInstanceOf[java.util.List[Double]].asScala shouldBe List(true, false, true)
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedListFoo](output)
//      avro.write(NestedListFoo(List(Foo("sam", false))))
//      avro.close()
//
//      val record = read[NestedListFoo](output)
//      record.get("foos").toString shouldBe """[{"str": "sam", "boolean": false}]"""
//    }
//    "" in {
//
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test(Map(("name", "sammy"))))
//      avro.close()
//
//      val record = read[Test](output)
//      record.get("map").asInstanceOf[java.util.Map[Utf8, Utf8]].asScala.map { case (k, v) =>
//        (k.toString, v.toString)
//      } shouldBe Map(("name", "sammy"))
//    }
//    "" in {
//
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test(Map(("name", 12.3d))))
//      avro.close()
//
//      val record = read[Test](output)
//      record.get("map").asInstanceOf[java.util.Map[Utf8, java.lang.Double]].asScala.map { case (k, v) =>
//        (k.toString, v.toString.toDouble)
//      } shouldBe Map(("name", 12.3d))
//    }
//    "" in {
//
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[Test](output)
//      avro.write(Test(Map(("name", true))))
//      avro.close()
//
//      val record = read[Test](output)
//      record.get("map").asInstanceOf[java.util.Map[Utf8, java.lang.Boolean]].asScala.map { case (k, v) =>
//        (k.toString, v.toString.toBoolean)
//      } shouldBe Map(("name", true))
//    }
//    "" in {
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[NestedMapTest](output)
//      avro.write()
//      avro.close()
//
//      val record = read[NestedMapTest](output)
//      val map = record.get("map").asInstanceOf[java.util.Map[Utf8, GenericRecord]].asScala.map { case (k, v) =>
//        (k.toString, v)
//      }
//      map("foo").get("str").toString shouldBe "sam"
//      map("foo").get("boolean").toString shouldBe "false"
//    }
//    "support extends AnyVal" in {
//      val instance = ValueWrapper(ValueClass("bob"))
//
//      val output = new ByteArrayOutputStream
//      val avro = AvroOutputStream.data[ValueWrapper](output)
//      avro.write(instance)
//      avro.close()
//
//      val record = read[ValueWrapper](output)
//      new String(record.get("valueClass").asInstanceOf[Utf8].getBytes) shouldBe "bob"
//    }
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