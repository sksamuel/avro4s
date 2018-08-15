package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.{AvroAlias, AvroName}
import org.apache.avro.specific.FixedSize
import org.scalatest.{FunSuite, Matchers}

@FixedSize(12)
case class Artist(@AvroName("foo") name: String, birthplace: String, works: Seq[Painting])

case class Painting(name: String, @AvroAlias("y") year: Int)

class DataTypeForTest extends FunSuite with Matchers {

  test("basic case class") {
    DataTypeFor.apply[Painting].dataType shouldBe
      StructType(
        "com.sksamuel.avro4s.internal.Painting",
        annotations = List(),
        fields = List(
          StructField("name", StringType, List(), null),
          StructField("year", IntType, List(Anno("com.sksamuel.avro4s.AvroAlias", List("y"))), null)
        )
      )
  }

  test("seq of nested case classes") {
    DataTypeFor.apply[Artist].dataType shouldBe
      StructType(
        "com.sksamuel.avro4s.internal.Artist",
        annotations = List(Anno("org.apache.avro.specific.FixedSize", List("value = 12"))),
        fields = List(
          StructField("name", StringType, List(Anno("com.sksamuel.avro4s.AvroName", List("foo"))), null),
          StructField("birthplace", StringType, List(), null),
          StructField("works", ArrayType(
            StructType(
              "com.sksamuel.avro4s.internal.Painting",
              annotations = List(),
              fields = List(
                StructField("name", StringType, List(), null),
                StructField("year", IntType, List(Anno("com.sksamuel.avro4s.AvroAlias", List("y"))), null)
              )
            )
          ), List(), null)
        )
      )
  }
}
