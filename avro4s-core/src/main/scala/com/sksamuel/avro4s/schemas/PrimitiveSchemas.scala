package com.sksamuel.avro4s.schemas

import org.apache.avro.util.Utf8
import org.apache.avro.{LogicalTypes, SchemaBuilder}

import java.nio.ByteBuffer
import java.util.UUID

trait PrimitiveSchemas:

  given intSchemaFor: SchemaFor[Int] = SchemaFor[Int](SchemaBuilder.builder.intType)
  given SchemaFor[Byte] = intSchemaFor.forType
  given SchemaFor[Short] = intSchemaFor.forType
  given SchemaFor[Long] = SchemaFor[Long](SchemaBuilder.builder.longType)
  given SchemaFor[Float] = SchemaFor[Float](SchemaBuilder.builder.floatType)
  given SchemaFor[Double] = SchemaFor[Double](SchemaBuilder.builder.doubleType)
  given SchemaFor[scala.Boolean] = SchemaFor[Boolean](SchemaBuilder.builder.booleanType)
  given SchemaFor[ByteBuffer] = SchemaFor[ByteBuffer](SchemaBuilder.builder.bytesType)
  given stringSchemaFor: SchemaFor[String] = SchemaFor[String](SchemaBuilder.builder.stringType)
  given SchemaFor[Utf8] = stringSchemaFor.forType
  given SchemaFor[CharSequence] = stringSchemaFor.forType
  given SchemaFor[UUID] = SchemaFor[UUID](LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType))
