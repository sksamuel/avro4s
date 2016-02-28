package com.sksamuel.avro4s.json

import java.util

import org.apache.avro.Schema

/**
  * Accepts a json string, and returns an Avro Schema that best matches the json string.
  *
  * Converts:
  *
  * - json arrays to avro arrays
  * - objects to records
  * - doubles to doubles
  * - ints/longs to longs
  * - booleans to booleans
  * - nulls to union(string,null)
  */
class JsonToAvroConverter(namespace: String) {

  import org.json4s._
  import org.json4s.native.JsonMethods._

  import scala.collection.JavaConverters._

  def convert(name: String, str: String): Schema = convert(name, parse(str))

  def convert(name: String, value: JValue): Schema = value match {
    case JArray(value) => Schema.createArray(convert(name, value.head))
    case JBool(_) => Schema.create(Schema.Type.BOOLEAN)
    case JDecimal(_) => Schema.create(Schema.Type.DOUBLE)
    case JDouble(_) => Schema.create(Schema.Type.DOUBLE)
    case JInt(_) => Schema.create(Schema.Type.LONG)
    case JLong(_) => Schema.create(Schema.Type.LONG)
    case JNothing => Schema.create(Schema.Type.NULL)
    case JNull => Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)))
    case JString(_) => Schema.create(Schema.Type.STRING)
    case JObject(values) =>
      val record = Schema.createRecord(name, null, namespace, false)
      val fields = values.map { case (name, value) => new Schema.Field(name, convert(name, value), null, null) }
      record.setFields(fields.asJava)
      record
  }

}
