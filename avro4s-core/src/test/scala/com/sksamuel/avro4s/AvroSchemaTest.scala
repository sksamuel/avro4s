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

    //    "generate aliases when specified by Aliases annotation" in {
    //      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases.avsc"))
    //      val writer = schemaFor[AliasExample]
    //      writer.schema.toString(true) shouldBe expected.toString(true)
    //    }
    "nested seq in maps in classes" in {
      //   val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seq5.avsc"))
      //   val schema = schemaFor[SeqExample5]
      //   println(schema.schema.toString(true))
      //  schema.schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class MapExample(mymap: Map[String, Long])

case class SeqExample(seq: Seq[Double])

case class SeqExample2(seq: Seq[SeqExample])

case class SeqExample3(seq: Seq[ClassWithStringMap])

case class ClassWithStringMap(map: Map[String, String])

case class ClassWithComplexMap(map: Map[String, SeqExample2])

case class SeqExample5(seq: Seq[ClassWithStringMap])

case class ArrayExample(array: Seq[Boolean])

case class ListExample(list: Seq[Float])

case class OptionExample(option: Option[Array[Byte]])

case class EitherExample(either: Either[String, Double])

case class AliasExample(@AvroAlias("tother", "tnext") field: String)

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