package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.avroutils.SchemaHelper
import com.sksamuel.avro4s.{Avro4sException, FieldMapper, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}

trait OptionSchemas {

  given SchemaFor[None.type] = NoneSchemaFor

  given[T](using schemaFor: SchemaFor[T]): SchemaFor[Option[T]] = new SchemaFor[Option[T]] {
    override def schema: Schema = {
      val rhs: Schema = schemaFor.schema
      if (rhs.isUnion)
        throw new Avro4sException("Options cannot contain other union types, such as sealed traits")
      Schema.createUnion(Schema.create(Schema.Type.NULL), schemaFor.schema)
    }
  }
}

object NoneSchemaFor extends SchemaFor[None.type] :
  private val s = SchemaBuilder.builder.nullType
  override def schema: Schema = s