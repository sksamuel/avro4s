package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util.UUID

trait StringEncoders:

  given stringEncoder: Encoder[String] = new Encoder[String] {
    override def encode(schema: Schema): String => Any = schema.getType match {
      case Schema.Type.STRING => a => new Utf8(a.toString)
      case Schema.Type.BYTES => a => a.toString().getBytes
      case Schema.Type.FIXED => { a =>
        if (a.getBytes.length > schema.getFixedSize)
          throw new Avro4sEncodingException(s"Cannot write string with ${a.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
        GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(a.getBytes).array, schema).asInstanceOf[GenericData.Fixed]
      }
      case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
    }
  }

  given Encoder[Utf8] = Encoder.identity
  given Encoder[CharSequence] = Encoder(a => a.toString)
  given Encoder[UUID] = Encoder(x => x.toString)
  