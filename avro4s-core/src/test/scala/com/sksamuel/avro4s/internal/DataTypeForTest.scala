package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.{AvroAlias, AvroName}
import org.apache.avro.specific.FixedSize
import org.scalatest.{FunSuite, Matchers}

@FixedSize(12)
case class Artist(@AvroName("foo") name: String, birthplace: String, works: Seq[Painting])
case class Painting(name: String, @AvroAlias("y") year: Int)

sealed trait Style
case object Impressionist extends Style
case object Romanticist extends Style

case class Movement(style: Style, startYear: Int)

class DataTypeForTest extends FunSuite with Matchers {

  test("case class") {
    DataTypeFor.apply[Painting].dataType shouldBe
      StructType(
        "com.sksamuel.avro4s.internal.Painting",
        "Painting",
        "com.sksamuel.avro4s.internal",
        annotations = List(),
        fields = List(
          StructField("name", StringType),
          StructField("year", IntType, List(Anno("com.sksamuel.avro4s.AvroAlias", List("y"))), null)
        )
      )
  }

  test("seq of nested case classes") {
    DataTypeFor.apply[Artist].dataType shouldBe
      StructType(
        "com.sksamuel.avro4s.internal.Artist",
        "Artist",
        "com.sksamuel.avro4s.internal",
        annotations = List(Anno("org.apache.avro.specific.FixedSize", List("value = 12"))),
        fields = List(
          StructField("name", StringType, List(Anno("com.sksamuel.avro4s.AvroName", List("foo"))), null),
          StructField("birthplace", StringType, List(), null),
          StructField("works", ArrayType(
            StructType(
              "com.sksamuel.avro4s.internal.Painting",
              "Painting",
              "com.sksamuel.avro4s.internal",
              annotations = List(),
              fields = List(
                StructField("name", StringType),
                StructField("year", IntType, List(Anno("com.sksamuel.avro4s.AvroAlias", List("y"))))
              )
            )
          ), List(), null)
        )
      )
  }

  test("ADT of sealed trait with objects") {
    DataTypeFor.apply[Movement].dataType shouldBe
      StructType(
        "com.sksamuel.avro4s.internal.Movement",
        "Movement",
        "com.sksamuel.avro4s.internal",
        annotations = List(),
        List(
          StructField("style", EnumType("com.sksamuel.avro4s.internal.Style", "Style", "com.sksamuel.avro4s.internal", List("Impressionist", "Romanticist"), Nil), List(), null),
          StructField("startYear", IntType)
        )
      )
  }

  test("case classes defined inside methods") {
    case class Foo(a: String)
    DataTypeFor.apply[Foo].dataType shouldBe
      StructType(
        "com.sksamuel.avro4s.internal.DataTypeForTest.Foo",
        "Foo",
        "com.sksamuel.avro4s.internal",
        annotations = List(),
        List(
          StructField("a", StringType)
        )
      )
  }
}
