package com.sksamuel.avro4s.github

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.sksamuel.avro4s.record.decoder.CPWrapper
import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import shapeless.Coproduct

class GithubIssue485 extends AnyFunSuite with Matchers {

  test("Serializable Coproduct Decoder #485") {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(Decoder[CPWrapper])
    oos.close()

    val decoder =
      new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray)).readObject().asInstanceOf[Decoder[CPWrapper]]

    val schema = AvroSchema[CPWrapper]
    val record = new GenericData.Record(schema)
    record.put("u", new Utf8("wibble"))
    decoder.decode(record, schema, DefaultFieldMapper) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG]("wibble"))
  }
}
