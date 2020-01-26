package com.sksamuel.avro4s.github

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroOutputStream, AvroSchemaV2}
import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final case class SN(value: String) extends AnyVal
final case class SimpleUser(name: String, sn: Option[SN])

class GithubIssue191 extends AnyFunSuite with Matchers {

  test("writing out AnyVal in an option") {
    implicit val schema = AvroSchemaV2[SimpleUser]
    val bytes = new ByteArrayOutputStream
    val out = AvroOutputStream.data[SimpleUser].to(bytes).build()
    out.write(SimpleUser("Tom", Some(SN("123"))))
    out.close()

    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes.toByteArray), datumReader)
    val record = new Iterator[GenericRecord] {
      override def hasNext: Boolean = dataFileReader.hasNext
      override def next(): GenericRecord = dataFileReader.next
    }.toList.head
    record.getSchema shouldBe schema
    record.get("name") shouldBe new Utf8("Tom")
    record.get("sn") shouldBe new Utf8("123")
  }
}
