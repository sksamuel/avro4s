package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder, EncoderFor}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util.UUID

trait StringEncoders:

  given stringEncoderFor: EncoderFor[String] = new EncoderFor[String] {

    override def encoder(schema: Schema): Encoder[String] = schema.getType match {
      case Schema.Type.STRING => Encoder(a => new Utf8(a.toString))
      case Schema.Type.BYTES => Encoder(a => a.toString().getBytes)
      case Schema.Type.FIXED => new Encoder[String] {
        override def encode(value: String): Any = {
          if (value.getBytes.length > schema.getFixedSize)
            throw new Avro4sEncodingException(s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
          GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema).asInstanceOf[GenericData.Fixed]
        }
      }
      case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema") 
    }
  }

  given EncoderFor[Utf8] = EncoderFor.identity
  given EncoderFor[CharSequence] = EncoderFor(a => a.toString)
  given EncoderFor[UUID] = EncoderFor(x => x.toString)