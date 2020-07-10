package com.sksamuel.avro4s.github

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.sksamuel.avro4s.record.decoder.CPWrapper
import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import shapeless.Coproduct

class GithubIssue510 extends AnyFunSuite with Matchers {

  test("AvroOutputStream should work for Vector[Int] #510") {
    val obj = Vector[Int](1)
    val byteStr = new java.io.ByteArrayOutputStream
    val avroStr =  AvroOutputStream.binary[Vector[Int]].to(byteStr).build()
    avroStr.write(obj)
    avroStr.close()

    println(byteStr.toByteArray.map("%02X" format _).mkString)
  }
}
