package com.sksamuel.avro4s.examples

import org.scalatest.{Matchers, WordSpec}

/**
  *
  * Example of how to map sealed trait+case object style of enumeration to
  * avro.
  *
  * API inspired by scodec
  *
  * Design goals were to avoid the three individual implicits required by README "custom type mapping" example
  * and avoid redundant specification of the object/Int mapping.
  *
  */
class TraitObjectEnumerationExample extends WordSpec with Matchers {

  sealed trait Base

  case object A extends Base

  case object B extends Base

  case class Test(v: Base)


  object Pack {

    import org.apache.avro.Schema
    import org.apache.avro.Schema.Field
    import com.sksamuel.avro4s.{FromValue, ToValue, SchemaFor}

    def apply[T](mapping: (T, Int)*) = new Pack(
      Map(mapping: _*),
      Map((for ((k, v) <- mapping) yield (v, k)): _*))

    class Pack[T](to: Map[T, Int], from: Map[Int, T]) extends SchemaFor[T] with ToValue[T] with FromValue[T] {
      def apply = Schema.create(Schema.Type.INT)

      override def apply(value: T): Int = to(value)

      override def apply(value: Any, field: Field): T = from(value.asInstanceOf[Int])
    }

  }

  implicit val BasePack = Pack[Base](A -> 0, B -> 1)

  import scala.util.Success
  import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
  import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema}

  "AvroStream" should {

    "generate schema using int " in {
      AvroSchema[Test].toString should be(
        """{"type":"record","name":"Test","namespace":"com.sksamuel.avro4s.examples","fields":[{"name":"v","type":"int"}]}""")
    }

    "serialize as int" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.json[Test](baos)
      output.write(Test(A))
      output.close()
      baos.toString("UTF-8") shouldBe ("""{"v":0}""")
    }

    "deserialize from int" in {
      val json = """{"v":1}"""

      val in = new ByteArrayInputStream(json.getBytes("UTF-8"))
      val input = AvroInputStream.json[Test](in)
      input.singleEntity shouldBe (Success(Test(B)))
    }
  }
}
