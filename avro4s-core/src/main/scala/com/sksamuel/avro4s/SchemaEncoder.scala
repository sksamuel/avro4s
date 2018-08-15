package com.sksamuel.avro4s

import java.io.Serializable

import com.sksamuel.avro4s.internal._
import org.apache.avro.Schema

trait SchemaEncoder[A] extends Serializable {
  def encode: Schema
}

object SchemaEncoder {
  implicit def apply[T](implicit dataTypeFor: DataTypeFor[T]): SchemaEncoder[T] = new SchemaEncoder[T] {
    override val encode: Schema = {
      dataTypeFor.dataType match {
        case StructType(className, annos, fields) => Schema.create(Schema.Type.BOOLEAN)
        case StringType => Schema.create(Schema.Type.STRING)
        case BooleanType => Schema.create(Schema.Type.BOOLEAN)
        case DoubleType => Schema.create(Schema.Type.DOUBLE)
        case FloatType => Schema.create(Schema.Type.FLOAT)
        case LongType => Schema.create(Schema.Type.LONG)
        case _ => Schema.create(Schema.Type.STRING)
      }
    }
  }
}
