package com.sksamuel.avro4s.schemas

import org.apache.avro.util.Utf8
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import com.sksamuel.avro4s.{SchemaConfiguration, SchemaFor}

import java.nio.ByteBuffer
import java.util.UUID

trait StringSchemas:
  given StringSchemaFor: SchemaFor[String] = SchemaFor[String](SchemaBuilder.builder.stringType)
  given SchemaFor[Utf8] = StringSchemaFor.forType
  given SchemaFor[CharSequence] = StringSchemaFor.forType
  given SchemaFor[UUID] = SchemaFor[UUID](LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType))

  def fixedStringSchemaFor(name: String, size: Int): SchemaFor[String] = SchemaFor(SchemaBuilder.fixed(name).size(size))

/**
  * Returns a [[Schema]] for strings that uses avro.java.string to specify that java strings should be used.
  */
object JavaStringSchemaFor extends SchemaFor[String] {
  val s = SchemaBuilder.builder.stringBuilder().prop("avro.java.string", "String").endString()
  override def schema: Schema = s
}