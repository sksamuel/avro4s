package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.Recursive.MutRec1
import com.sksamuel.avro4s.{AvroSchema, Recursive}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RecursiveSchemaTest extends AnyWordSpec with Matchers {

  "SchemaFor" should {
    "support recursive types with sealed traits" in {
      AvroSchema[Recursive.Tree[Int]] shouldBe expectedSchema("/recursive_tree.json")
    }

    "support mutually recursive types" in {
      AvroSchema[MutRec1] shouldBe expectedSchema("/mutually_recursive.json")
    }

    "support recursive types with lists" in {
      AvroSchema[Recursive.ListTree[Int]] shouldBe expectedSchema("/recursive_list.json")
    }

    "support recursive types with maps" in {
      AvroSchema[Recursive.MapTree[Int]] shouldBe expectedSchema("/recursive_map.json")
    }

    "support recursive types with option" in {
      AvroSchema[Recursive.OptionTree[Int]] shouldBe expectedSchema("/recursive_option.json")
    }

    "support recursive types with either" in {
      AvroSchema[Recursive.EitherTree[Int]] shouldBe expectedSchema("/recursive_either.json")
    }

    "support recursive types with shapeless coproduct" in {
      AvroSchema[Recursive.CoproductTree[Int]] shouldBe expectedSchema("/recursive_coproduct.json")
    }

    "support recursive types with tuples and value types" in {
      AvroSchema[Recursive.TVTree[Int]] shouldBe expectedSchema("/recursive_tuple_value_type.json")
    }
  }

  def expectedSchema(name: String) =
    new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(name))

}
