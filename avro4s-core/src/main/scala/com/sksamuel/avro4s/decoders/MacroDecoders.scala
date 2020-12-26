package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{DefaultFieldMapper, FieldMapper}
import org.apache.avro.Schema

import scala.deriving.Mirror

trait MacroDecoders {
  inline given derived[T](using m: Mirror.Of[T]): Decoder[T] = new Decoder[T] {
    override def decode(schema: Schema, mapper: FieldMapper = DefaultFieldMapper): Any => T = { value => value.asInstanceOf[T] }
  }
}
