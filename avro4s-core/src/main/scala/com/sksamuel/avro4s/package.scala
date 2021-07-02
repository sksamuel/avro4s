package com.sksamuel.avro4s

import org.apache.avro.generic.IndexedRecord

extension [T: Encoder: SchemaFor](o: T)
  def asRecord: Record = Encoder[T].encode(SchemaFor[T].schema)(o) match
    case record: Record => record
    case output =>
      val clazz = output.getClass
      throw new Avro4sEncodingException(s"Cannot marshall an instance of $o to a Record (had class $clazz, output was $output)")

extension (record: IndexedRecord)
  def as[T: Decoder: SchemaFor]: T = Decoder[T].decode(SchemaFor[T].schema).apply(record)
