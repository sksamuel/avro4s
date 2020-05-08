package com.sksamuel.avro4s.github

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.sksamuel.avro4s.record.decoder.ScalaEnumClass
import com.sksamuel.avro4s.schema.Colours
import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.EnumSymbol
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GithubIssue484 extends AnyFunSuite with Matchers {

  test("Serializable Scala Enum Decoder #484") {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(Decoder[ScalaEnumClass])
    oos.close()

    val decoder = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
      .readObject()
      .asInstanceOf[Decoder[ScalaEnumClass]]

    val schema = AvroSchema[ScalaEnumClass]
    val record = new GenericData.Record(schema)
    record.put("colour", new EnumSymbol(schema.getField("colour").schema(), "Green"))
    decoder.decode(record, schema, DefaultFieldMapper) shouldBe ScalaEnumClass(Colours.Green)
  }
}
