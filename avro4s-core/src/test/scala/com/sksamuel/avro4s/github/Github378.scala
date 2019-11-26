package com.sksamuel.avro4s.github

import java.time.ZonedDateTime

import com.sksamuel.avro4s.{AvroDefault, AvroName, AvroSchema, Decoder, Encoder, FieldMapper, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FunSuite, Matchers}

class Github378 extends FunSuite with Matchers {

  implicit object ZonedDateTimeSchemaFor extends SchemaFor[ZonedDateTime] {
    override def schema(fieldMapper: FieldMapper): Schema =
      SchemaBuilder.builder.stringType
  }

  implicit object ZonedDateTimeEncoder extends Encoder[ZonedDateTime] {
    override def encode(t: ZonedDateTime, schema: Schema, fieldMapper: FieldMapper): AnyRef =
      t.toString
  }

  implicit object ZonedDateTimeDecoder extends Decoder[ZonedDateTime] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): ZonedDateTime =
      ZonedDateTime.parse(value.toString)
  }

  test("Error building schemas with custom types serialized as Strings") {
    AvroSchema[Github378Model] should be (AvroSchema[Github378ModelAsString])
  }

  test("default values with int type") {
    AvroSchema[Github378IntModel] should be(AvroSchema[Github378IntWithAnnotationModel])
  }

  test("default values with long type") {
    AvroSchema[Github378LongModel] should be(AvroSchema[Github378LongWithAnnotationModel])
  }

  test("default values with float type") {
    AvroSchema[Github378FloatModel] should be(AvroSchema[Github378FloatWithAnnotationModel])
  }

  test("default values with double type") {
    AvroSchema[Github378DoubleModel] should be(AvroSchema[Github378DoubleWithAnnotationModel])
  }

  test("default values with boolean type") {
    AvroSchema[Github378BooleanModel] should be(AvroSchema[Github378BooleanWithAnnotationModel])
  }
}

final case class Github378Model(@AvroDefault("2019-11-26T18:22:36.519Z") date: ZonedDateTime)
@AvroName("Github378Model")
final case class Github378ModelAsString(date: String = "2019-11-26T18:22:36.519Z")

final case class Github378IntModel(value: Int = 42)
@AvroName("Github378IntModel")
final case class Github378IntWithAnnotationModel(@AvroDefault("42") value: Int)

final case class Github378LongModel(value: Long = 42L)
@AvroName("Github378LongModel")
final case class Github378LongWithAnnotationModel(@AvroDefault("42") value: Long)

final case class Github378FloatModel(value: Float = 42.0F)
@AvroName("Github378FloatModel")
final case class Github378FloatWithAnnotationModel(@AvroDefault("42") value: Float)

final case class Github378DoubleModel(value: Double = 42.0D)
@AvroName("Github378DoubleModel")
final case class Github378DoubleWithAnnotationModel(@AvroDefault("42") value: Double)

final case class Github378BooleanModel(value: Boolean = true)
@AvroName("Github378BooleanModel")
final case class Github378BooleanWithAnnotationModel(@AvroDefault("true") value: Boolean)



