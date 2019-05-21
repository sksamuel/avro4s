package com.sksamuel.avro4s.github

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroAlias, AvroInputStream, AvroOutputStream, AvroSchema}
import org.scalatest.{FunSuite, Matchers}

class Github202 extends FunSuite with Matchers {

  ignore("Alias avro schema feature problem #202") {

    case class Version1(a: String)

    case class Version2(@AvroAlias("a") renamed: String, `new`: String = "default")

    println(AvroSchema[Version1].toString(true))
    println(AvroSchema[Version2].toString(true))

    val v1 = Version1("hello")
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.data[Version1].to(baos).build(AvroSchema[Version1])
    output.write(v1)
    output.close()

    println(baos.toByteArray.toList)

    val is = AvroInputStream.data[Version2].from(baos.toByteArray).build(AvroSchema[Version1], AvroSchema[Version2])
    val v2 = is.iterator.toList.head
    is.close()

    assert(v2.renamed == v1.a)
    assert(v2.`new` == "default")
  }
}
