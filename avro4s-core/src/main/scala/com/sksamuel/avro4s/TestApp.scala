package com.sksamuel.avro4s

import java.util

import org.apache.avro.{Schema, SchemaBuilder}

object TestApp extends App {

  private val schema = SchemaBuilder.builder().longType()
  private val field1 = new Schema.Field("boo", schema, null, null: Any)
  private val field2 = new Schema.Field("woo", schema, null, null: Any)
  val record = Schema.createRecord(util.Arrays.asList(field1, field2))
  println(record)
}
