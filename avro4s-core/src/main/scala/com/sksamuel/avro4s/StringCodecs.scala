package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.util.Utf8

trait StringCodecs {

  sealed trait StringCodecBase extends Codec[String] with FieldSpecificCodec[String] {
    def forFieldWith(schema: Schema, annotations: Seq[Any]): Codec[String] = schema.getType match {
      case Schema.Type.STRING => new StringCodec(schema)
      case Schema.Type.FIXED  => new FixedStringCodec(schema)
      case Schema.Type.BYTES  => new ByteStringCodec(schema)
      case _                  => sys.error(s"Unsupported type for string schema: $schema")
    }
  }

  implicit val stringCodec = new StringCodec(SchemaBuilder.builder.stringType)

  class StringCodec(val schema: Schema) extends StringCodecBase {
    require(schema.getType == Schema.Type.STRING)

    def encode(value: String): AnyRef = new Utf8(value)

    def decode(value: Any): String = value match {
      case u: Utf8             => u.toString
      case s: String           => s
      case chars: CharSequence => chars.toString
      case null                => sys.error("Cannot decode <null> as a string")
      case other               => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
    }
  }

  class FixedStringCodec(val schema: Schema) extends StringCodecBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: String): AnyRef = {
      if (value.getBytes.length > schema.getFixedSize)
        sys.error(
          s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema)
    }

    def decode(value: Any): String = value match {
      case fixed: GenericFixed => new String(fixed.bytes())
      case null                => sys.error("Cannot decode <null> as a string")
      case other               => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
    }
  }

  class ByteStringCodec(val schema: Schema) extends Codec[String] {
    require(schema.getType == Schema.Type.BYTES)

    def encode(value: String): AnyRef = ByteBuffer.wrap(value.getBytes)

    def decode(value: Any): String = value match {
      case bytebuf: ByteBuffer => new String(bytebuf.array)
      case a: Array[Byte]      => new String(a)
      case null                => sys.error("Cannot decode <null> as a string")
      case other               => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
    }
  }
}
