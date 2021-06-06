package com.sksamuel.avro4s.avroutils

import org.apache.avro.{Schema, SchemaBuilder}
import collection.JavaConverters.asScalaBufferConverter

object EnumHelpers {

  /**
    * Returns an enum schema which is a copy of this enum schema, adding the given default.
    */
  def addDefault[E](default: E)(schema: Schema): Schema =
    require(schema.getType == Schema.Type.ENUM)
    SchemaBuilder
      .enumeration(schema.getName)
      .namespace(schema.getNamespace)
      .defaultSymbol(default.toString)
      .symbols(schema.getEnumSymbols.asScala.toList: _*)
}
