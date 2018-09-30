
//class AvroOutputStreamTest extends WordSpec with Matchers with TimeLimits {
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
//  }
//}
//