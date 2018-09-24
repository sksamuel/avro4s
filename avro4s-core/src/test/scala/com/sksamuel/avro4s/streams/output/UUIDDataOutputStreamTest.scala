package com.sksamuel.avro4s.streams.output

import java.util.UUID

import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class UUIDDataOutputStreamTest extends DataOutputStreamTest {

  import scala.collection.JavaConverters._

  test("write out uuids") {
    val a = UUID.randomUUID()
    case class Test(z: UUID)
    val out = write(Test(a))
    val record = read[Test](out)
    record.get("z") shouldBe new Utf8(a.toString)
  }

  test("write out seq of UUIDS") {
    case class Test(z: Seq[UUID])
    val a = UUID.randomUUID()
    val b = UUID.randomUUID()
    val c = UUID.randomUUID()
    val out = write(Test(Seq(a, b, c)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericData.Array[Utf8]].asScala shouldBe Seq(a, b, c).map(_.toString).map(new Utf8(_))
  }

  test("write out Some[UUID]") {
    val a = UUID.randomUUID()
    case class Test(z: Option[UUID])
    val out = write(Test(Some(a)))
    val record = read[Test](out)
    record.get("z") shouldBe new Utf8(a.toString)
  }

  test("write out None[UUID]") {
    case class Test(z: Option[UUID])
    val out = write(Test(None))
    val record = read[Test](out)
    record.get("z") shouldBe null
  }

  test("write out UUID with default value") {
    case class Test(z: UUID = UUID.fromString("86da265c-95bd-443c-8860-9381efca059d"))
    val out = write(Test())
    val record = read[Test](out)
    record.get("z") shouldBe new Utf8("86da265c-95bd-443c-8860-9381efca059d")
  }

}