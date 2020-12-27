package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s._
import com.sksamuel.avro4s.encoders.Encoder
import com.sksamuel.avro4s.schemas.SchemaFor
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OverrideEncoderImplicitTest extends AnyWordSpec with Matchers {

  "Encoder" should {
    "support overriding by providing an in scope implicit" in {
      case class Foo(s: String)

      val fixedSchema = SchemaFor[String](Schema.createFixed("FixedString", null, null, 7))
      val schema = AvroSchema[Foo]

//      implicit val fixedStringEncoder = Encoder.stringEncoder.encode(fixedSchema.schema(SchemaConfiguration.default))

//      val record = Encoder[Foo].encode(Foo("hello")).asInstanceOf[GenericRecord]
//      record.get("s").asInstanceOf[GenericFixed].bytes().toList shouldBe Seq(104, 101, 108, 108, 111, 0, 0)
//      the fixed should have the right size
//      record.get("s").asInstanceOf[GenericFixed].bytes().length shouldBe 7
    }
  }
}
