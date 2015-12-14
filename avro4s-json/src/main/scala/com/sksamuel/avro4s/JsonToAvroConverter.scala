package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.Schema.Field

/**
  * Accepts a json string, and returns an Avro Schema that best matches the json string.
  *
  * Converts:
  *
  * - arrays to lists
  * - objects to case classes
  * - doubles to doubles
  * - ints to longs
  * - booleans to booleans
  * - nulls to Option[String]
  */
class JsonToAvroConverter {

  import org.json4s._
  import org.json4s.native.JsonMethods._
  import scala.collection.JavaConverters._

  def convert(namespace: String, name: String, value: JValue): Schema = {
    val schema = org.apache.avro.Schema.createRecord(name, null, namespace, false)
    schema.setFields(q.asJava)
    schema
  }

  def fields(value: JObject): Seq[Schema.Field] = {
    value.values.map {
      case (name, value) =>
    }
  }

  def convert(namespace: String, name: String, str: String): Schema = convert(namespace, name, parse(str))
}
