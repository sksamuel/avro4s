package com.sksamuel.avro4s.github

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroFixed, AvroInputStream, AvroOutputStream}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class Data(uuid: Option[UUID])
case class UUID(@AvroFixed(8) bytes: Array[Byte])

class GithubIssue193 extends AnyFunSuite with Matchers {

  test("Converting data with an optional fixed type field to GenericRecord fails #193") {

    val baos = new ByteArrayOutputStream()

    val output = AvroOutputStream.data[Data].to(baos).build()
    output.write(Data(Some(UUID(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7)))))
    output.write(Data(None))
    output.write(Data(Some(UUID(Array[Byte](7, 6, 5, 4, 3, 2, 1, 0)))))
    output.close()

    val input = AvroInputStream.data[Data].from(baos.toByteArray).build
    val datas = input.iterator.toList
    datas.head.uuid.get.bytes should equal(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))
    datas(1).uuid shouldBe None
    datas.last.uuid.get.bytes should equal(Array[Byte](7, 6, 5, 4, 3, 2, 1, 0))
    input.close()
  }
}
