package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

class SchemaMacroTest extends WordSpec with Matchers {

  import AvroImplicits._

  "SchemaGenerator.schemaFor" should {
    "generate correct schema" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/gameofthrones.avsc"))
      val writer = schemaFor[GameOfThrones]
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
    "generate map type for a scala.collection.immutable.Map" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/map.avsc"))
      val writer = schemaFor[MapExample]
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seq.avsc"))
      val writer = schemaFor[SeqExample]
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/array.avsc"))
      val writer = schemaFor[ArrayExample]
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/list.avsc"))
      val writer = schemaFor[ListExample]
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:null,T for Option[T]" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.avsc"))
      val writer = schemaFor[OptionExample]
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U]" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either.avsc"))
      val writer = schemaFor[EitherExample]
      println(writer.schema)
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class MapExample(mymap: Map[String, Long])

case class SeqExample(seq: Seq[Double])

case class ArrayExample(array: Seq[Boolean])

case class ListExample(list: Seq[Float])

case class OptionExample(option: Option[Array[Byte]])

case class EitherExample(either: Either[String, Double])

case class GameOfThrones(id: String,
                         kingdoms: Int,
                         rating: BigDecimal,
                         temperature: Double,
                         deathCount: Long,
                         aired: Boolean,
                         locations: Seq[String],
                         kings: Array[String],
                         seasons: List[Int],
                         alligence: Map[String, String],
                         throne: IronThrone,
                         houses: Seq[House])

case class IronThrone(swordCount: Int)

case class House(name: String, ruler: String)