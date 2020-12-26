package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util.UUID

object StringEncoder extends Encoder[String] {

  override def encode(schema: Schema, mapper: FieldMapper): String => Any = schema.getType match {
    case Schema.Type.STRING => { t => new Utf8(t) }
    case Schema.Type.BYTES => { t => ByteBuffer.wrap(t.getBytes) }
    case Schema.Type.FIXED => { t =>
      if (t.getBytes.length > schema.getFixedSize)
        throw new Avro4sEncodingException(s"Cannot write string with ${t.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(t.getBytes).array, schema).asInstanceOf[GenericData.Fixed]
    }
    case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
  }
}

trait StringEncoders:
  given Encoder[String] = StringEncoder
  given Encoder[Utf8] = Encoder.identity
  given Encoder[CharSequence] = Encoder(a => a.toString)
  given Encoder[UUID] = Encoder(x => x.toString)
  