//package com.sksamuel.avro4s.schema
//
//import com.sksamuel.avro4s.{AvroSchema, Recur, Recursive, Tree, SchemaFor}
//import org.apache.avro.{Schema, SchemaBuilder}
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import magnolia.*
//
//import java.time.LocalDate
//
//class RecursiveSchemaTest extends AnyWordSpec with Matchers {
//
//  "SchemaFor" should {
//
//    //    "support recursive case classes" in {
//    //      Schemas.derived[Foo].schema shouldBe SchemaBuilder.builder().stringType()
//    //      //      val schema = AvroSchema[Recur]
//    //      //      schema shouldBe expectedSchema("/recursive_tree.json")
//    //    }
//    //
//    //    "support recursive case classes2" in {
//    //      val schema = SchemaFor.derived[Recur].schema(null)
//    //      schema shouldBe expectedSchema("/recursive_tree.json")
//    //    }
//
//    //    "support recursive ADTs" in {
//    //      val schema = AvroSchema[Tree[Int]]
//    //      schema shouldBe expectedSchema("/recursive_tree.json")
//    //    }
//
//    //    "support mutually recursive types" in {
//    //      AvroSchema[Recursive.MutRec1] shouldBe expectedSchema("/mutually_recursive.json")
//    //    }
//    //
//    //    "support recursive types with lists" in {
//    //      AvroSchema.apply[Recursive.ListTree[Int]] shouldBe expectedSchema("/recursive_list.json")
//    //    }
//    //
//    //    "support recursive types with maps" in {
//    //      AvroSchema.apply[Recursive.MapTree[Int]] shouldBe expectedSchema("/recursive_map.json")
//    //    }
//    //
//    //    "support recursive types with option" in {
//    //      AvroSchema.apply[Recursive.OptionTree[Int]] shouldBe expectedSchema("/recursive_option.json")
//    //    }
//    //
//    //    "support recursive types with either" in {
//    //      AvroSchema.apply[Recursive.EitherTree[Int]] shouldBe expectedSchema("/recursive_either.json")
//    //    }
//
//    //    "support recursive types with shapeless coproduct" in {
//    //      AvroSchema[Recursive.CoproductTree[Int]] shouldBe expectedSchema("/recursive_coproduct.json")
//    //    }
//
//    //    "support recursive types with tuples and value types" in {
//    //      AvroSchema[Recursive.TVTree[Int]] shouldBe expectedSchema("/recursive_tuple_value_type.json")
//    //    }
//
//    //    "support custom definitions" in {
//    //      import scala.collection.JavaConverters._
//    //      implicit def sf: SchemaFor[Recursive.Branch[Int]] =
//    //        new ResolvableSchemaFor[Recursive.Branch[Int]] {
//    //          val tree = SchemaFor[Recursive.Tree[Int]]
//    //          def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[Branch[Int]] =
//    //            env.get[Recursive.Branch[Int]].getOrElse {
//    //
//    //              val record: SchemaFor[Recursive.Branch[Int]] =
//    //                SchemaFor(Schema.createRecord("CustomBranch", "custom schema", "custom", false))
//    //              val nextEnv = env.updated(record)
//    //              val treeSchema = tree.resolveSchemaFor(nextEnv, update).schema
//    //              val fields = Seq(new Schema.Field("left", treeSchema), new Schema.Field("right", treeSchema))
//    //              record.schema.setFields(fields.asJava)
//    //              record
//    //            }
//    //        }
//    //
//    //      val schema = sf.resolveSchemaFor().schema
//    //
//    //      schema shouldBe expectedSchema("/recursive_custom.json")
//    //    }
//  }
//
//  def expectedSchema(name: String): Schema =
//    new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(name))
//
//}
