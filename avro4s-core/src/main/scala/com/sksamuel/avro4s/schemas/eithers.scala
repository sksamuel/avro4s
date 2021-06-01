package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.avroutils.SchemaHelper
import com.sksamuel.avro4s.{SchemaConfiguration, SchemaFor}
import org.apache.avro.Schema

trait EitherSchemas:
  given[A, B](using leftSchemaFor: SchemaFor[A],
              rightSchemaFor: SchemaFor[B]): SchemaFor[Either[A, B]] =
    new SchemaFor[Either[A, B]] :
      override def schema(config: SchemaConfiguration) =
        SchemaFor(SchemaHelper.createSafeUnion(leftSchemaFor.schema(null), rightSchemaFor.schema(null))).schema(null)