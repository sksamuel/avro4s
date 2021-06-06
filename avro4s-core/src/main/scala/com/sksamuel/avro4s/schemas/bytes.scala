package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.SchemaBuilder

trait ByteIterableSchemas:
  given ByteArraySchemaFor: SchemaFor[Array[Byte]] = SchemaFor[Array[Byte]](SchemaBuilder.builder.bytesType)
  given ByteListSchemaFor: SchemaFor[List[Byte]] = ByteArraySchemaFor.forType
  given ByteSeqSchemaFor: SchemaFor[Seq[Byte]] = ByteArraySchemaFor.forType
  given ByteVectorSchemaFor: SchemaFor[Vector[Byte]] = ByteArraySchemaFor.forType