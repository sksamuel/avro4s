package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.encoders.{ByteStringEncoder, StringEncoder, UTF8StringEncoder}
import com.sksamuel.avro4s.{Avro4sConfigurationException, Avro4sDecodingException, Decoder, Encoder}
import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.util.Utf8
import org.apache.avro.{AvroRuntimeException, Schema}

import java.nio.ByteBuffer
import java.util.UUID

trait StringDecoders:
  given Decoder[String] = StringDecoder
  given Decoder[Utf8] = UTF8Decoder
  given Decoder[CharSequence] = CharSequenceDecoder
  given Decoder[UUID] = StringDecoder.map(UUID.fromString)

/**
  * A [[Decoder]] for Strings that pattern matches on the incoming type to decode.
  *
  * The schema is not used, meaning this decoder is forgiving of types that do not conform to
  * the schema, but are nevertheless useable.
  */
object StringDecoder extends Decoder[String] :
  override def decode(schema: Schema): Any => String = { string =>
    string match {
      case utf8: Utf8 => utf8.toString
      case string: String => string
      case charseq: CharSequence => charseq.toString
      case b: Array[Byte] => new Utf8(b).toString
      case bytes: ByteBuffer => new Utf8(bytes.array()).toString
      case fixed: GenericFixed => new Utf8(fixed.bytes()).toString
      case _ => throw new Avro4sDecodingException(s"Unsupported type $string ${string.getClass} for StringDecoder", string)
    }
  }

object CharSequenceDecoder extends Decoder[CharSequence]:
  override def decode(schema: Schema): Any => CharSequence = { string =>
    string match {
      case utf8: Utf8 => utf8
      case string: String => string
      case charseq: CharSequence => charseq
      case b: Array[Byte] => new Utf8(b)
      case bytes: ByteBuffer => new Utf8(bytes.array())
      case fixed: GenericFixed => new Utf8(fixed.bytes())
    }
  }

/**
  * A [[Decoder]] for UTF8 that pattern matches on the incoming type to decode.
  *
  * The schema is not used, meaning this decoder is forgiving of types that do not conform to
  * the schema, but are nevertheless useable.
  */
object UTF8Decoder extends Decoder[Utf8] :
  override def decode(schema: Schema): Any => Utf8 = { string =>
    string match {
      case utf8: Utf8 => utf8
      case string: String => new Utf8(string)
      case b: Array[Byte] => new Utf8(b)
      case bytes: ByteBuffer => new Utf8(bytes.array())
      case fixed: GenericFixed => new Utf8(fixed.bytes())
    }
  }

object StrictStringDecoder extends Decoder[String] :
  override def decode(schema: Schema): Any => String = schema.getType match {
    case Schema.Type.STRING => UTF8StringDecoder.decode(schema)
    case Schema.Type.BYTES => ByteStringDecoder.decode(schema)
    case Schema.Type.FIXED => GenericFixedStringDecoder.decode(schema)
    case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
  }

/**
  * A [[Decoder]] for Strings that decodes from avro's [[Utf8]]s.
  */
object UTF8StringDecoder extends Decoder[String] :
  override def decode(schema: Schema): Any => String = { input =>
    input match {
      case utf8: Utf8 => utf8.toString
    }
  }

/**
  * A [[Decoder]] for Strings that decodes from [[ByteBuffer]]s.
  */
object ByteStringDecoder extends Decoder[String] :
  override def decode(schema: Schema): Any => String = { input =>
    input match {
      case b: Array[Byte] => new Utf8(b).toString
      case bytes: ByteBuffer => new Utf8(bytes.array()).toString
    }
  }

/**
  * A [[Decoder]] for Strings that decodes from [[GenericFixed]]s.
  */
object GenericFixedStringDecoder extends Decoder[String] :
  override def decode(schema: Schema): Any => String = {
    require(schema.getType == Schema.Type.FIXED)
    { input =>
      input match {
        case fixed: GenericFixed => new Utf8(fixed.bytes()).toString
      }
    }
  }