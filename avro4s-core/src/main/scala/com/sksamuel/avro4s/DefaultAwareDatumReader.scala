package com.sksamuel.avro4s

import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.ResolvingDecoder
import org.apache.avro.{AvroTypeException, Schema}

class DefaultAwareDatumReader[T](writer: Schema, reader: Schema, data: GenericData)
  extends GenericDatumReader[T](writer, reader, data) {
  override def readField(r: scala.Any,
                         f: Schema.Field,
                         oldDatum: scala.Any,
                         in: ResolvingDecoder,
                         state: scala.Any): Unit = {
    try {
      super.readField(r, f, oldDatum, in, state)
    } catch {
      case t: AvroTypeException =>
        if (f.defaultVal == null) throw t else getData.setField(r, f.name, f.pos, f.defaultVal)
    }
  }
}