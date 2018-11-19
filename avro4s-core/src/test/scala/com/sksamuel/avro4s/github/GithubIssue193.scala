package com.sksamuel.avro4s.github

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s._
import org.scalatest.{FunSuite, Matchers}

@AvroName("Data")
case class Data(@AvroName("name")uuid: Option[UUID])
case class UUID(@AvroFixed(8) bytes: Array[Byte])

class GithubIssue193 extends FunSuite with Matchers {

  test("Converting data with an optional fixed type field to GenericRecord fails #193") {

    val baos = new ByteArrayOutputStream()

    val output = AvroOutputStream.data[Data].to(baos).build(AvroSchema[Data])
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
