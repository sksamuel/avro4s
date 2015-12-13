package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroInputStreamTest extends WordSpec with Matchers with Timeouts {

  def write[T](ts: Seq[T])(implicit schema: AvroSchema[T], ser: AvroSerializer[T]): Array[Byte] = {
    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream[T](output)
    avro.write(ts)
    avro.close()
    output.toByteArray
  }

  "AvroDeserializer" should {
    "read maps of booleans" in {
      case class Test(map: Map[String, Boolean])

      val data = Seq(Test(Map("sammy" -> true, "hammy" -> false)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of seqs of strings" in {
      case class Test(map: Map[String, Seq[String]])

      val data = Seq(Test(Map("sammy" -> Seq("foo", "moo"), "hammy" -> Seq("boo", "goo"))))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of options" in {
      case class Test(map: Map[String, Option[String]])

      val data = Seq(Test(Map("sammy" -> None, "hammy" -> Some("foo"))))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of case classes" in {
      case class Nested(double: Double)
      case class Test(map: Map[String, Nested])

      val data = Seq(Test(Map("sammy" -> Nested(124.5), "hammy" -> Nested(9))))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of strings" in {
      case class Test(map: Map[String, String])

      val data = Seq(Test(Map("sammy" -> "foo", "hammy" -> "boo")))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read maps of ints" in {
      case class Test(map: Map[String, Int])

      val data = Seq(Test(Map("sammy" -> 1, "hammy" -> 2)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of ints" in {
      case class Test(list: List[Int])

      val data = Seq(Test(List(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of doubles" in {
      case class Test(list: List[Double])

      val data = Seq(Test(List(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of strings" in {
      case class Test(list: List[String])

      val data = Seq(Test(List("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read list of case classes" in {
      case class Nested(str: String)
      case class Test(list: List[Nested])

      val data = Seq(Test(List(Nested("sammy"), Nested("hammy"))))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read array of ints" in {
      case class Test(array: Array[Int])

      val data = Seq(Test(Array(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of doubles" in {
      case class Test(array: Array[Double])

      val data = Seq(Test(Array(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of strings" in {
      case class Test(array: Array[String])

      val data = Seq(Test(Array("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read array of case classes" in {
      case class Nested(str: String)
      case class Test(array: Array[Nested])

      val data = Seq(Test(Array(Nested("sammy"), Nested("hammy"))))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList.head.array.toList shouldBe data.toList.head.array.toList
      in.close()
    }
    "read seq of ints" in {
      case class Test(opt: Seq[Int])

      val data = Seq(Test(Seq(1, 2, 3, 4)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of doubles" in {
      case class Test(opt: Seq[Double])

      val data = Seq(Test(Seq(0.1, 0.2, 0.3)))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of strings" in {
      case class Test(opt: Seq[String])

      val data = Seq(Test(Seq("sammy", "hammy")))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read seq of case classes" in {
      case class Nested(str: String)
      case class Test(opt: Seq[Nested])

      val data = Seq(Test(Seq(Nested("sammy"), Nested("hammy"))))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of case classes" in {
      case class Nested(str: String)
      case class Test(opt: Option[Nested])

      val data = Seq(Test(Option(Nested("sammy"))), Test(None))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of strings" in {
      case class Test(opt: Option[String])

      val data = Seq(Test(Option("sammy")), Test(None))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of booleans" in {
      case class Test(opt: Option[Boolean])

      val data = Seq(Test(Option(true)), Test(None))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read options of longs" in {
      case class Test(opt: Option[Long])

      val data = Seq(Test(Option(4)), Test(None))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read strings" in {
      case class Test(str: String)

      val data = Seq(Test("sammy"), Test("hammy"))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read booleans" in {
      case class Test(bool: Boolean)

      val data = Seq(Test(true), Test(false))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read doubles" in {
      case class Test(double: Double)

      val data = Seq(Test(1.2d), Test(2.3d))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read floats" in {
      case class Test(float: Float)

      val data = Seq(Test(1.2f), Test(3.4f))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read ints" in {
      case class Test(int: Int)

      val data = Seq(Test(1), Test(2))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
    "read longs" in {
      case class Test(long: Long)

      val data = Seq(Test(1l), Test(2l))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
  }
}