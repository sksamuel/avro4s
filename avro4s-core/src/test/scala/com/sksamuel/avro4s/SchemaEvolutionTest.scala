package com.sksamuel.avro4s

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{FunSuite, Matchers}

class SchemaEvolutionTest extends FunSuite with Matchers {

  case class Version1(original: String)
  case class Version2(@AvroAlias("original") renamed: String)

  case class P1(name: String, age: Int = 18)
  case class P2(name: String)

  ignore("@AvroAlias should be used when a reader schema has a field missing from the write schema") {

    println(AvroSchema[Version1].toString(true))
    println(AvroSchema[Version2].toString(true))

    val v1schema = AvroSchema[Version1]
    val v1 = Version1("hello")
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.data[Version1](baos, v1schema)
    output.write(v1)
    output.close()

    // we load using a v2 schema
    val v2schema = AvroSchema[Version2]
    val is = new AvroDataInputStream[Version2](new ByteArrayInputStream(baos.toByteArray), Some(v1schema), Some(v2schema))
    val v2 = is.iterator.toList.head

    v2.renamed shouldBe v1.original
  }

  test("when decoding, if the record/schema is missing a field and the target has a scala default, use that") {

    val f1 = RecordFormat[P1]
    val f2 = RecordFormat[P2]

    f1.from(f2.to(P2("foo"))) shouldBe P1("foo")
  }
}