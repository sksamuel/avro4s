package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Avro4sConfigurationException, Avro4sEncodingException, Encoder, FieldMapper}
import org.apache.avro.Conversions.UUIDConversion
import org.apache.avro.{Conversions, LogicalTypes, Schema}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

trait StringEncoders:
  given Encoder[String] = StringEncoder
  given Encoder[Utf8] = Encoder.identity
  given Encoder[CharSequence] = StringEncoder.contramap(_.toString())
  given Encoder[UUID] = UUIDEncoder

object StringEncoder extends Encoder[String] :
  override def encode(schema: Schema): String => Any = schema.getType match {
    case Schema.Type.STRING if schema.getObjectProp("avro.java.string") == "String" => Encoder.identity[String].encode(schema)
    case Schema.Type.STRING => UTF8StringEncoder.encode(schema)
    case Schema.Type.BYTES => ByteStringEncoder.encode(schema)
    case Schema.Type.FIXED => FixedStringEncoder.encode(schema)
    case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
  }

object UUIDEncoder extends Encoder[UUID] :
  override def encode(schema: Schema): UUID => Any = uuid => new Utf8(uuid.toString)

/**
  * An [[Encoder]] for Strings that encodes as avro [[Utf8]]s.
  */
object UTF8StringEncoder extends Encoder[String] :
  override def encode(schema: Schema): String => Any = string => new Utf8(string)

/**
  * An [[Encoder]] for Strings that encodes as [[ByteBuffer]]s.
  */
object ByteStringEncoder extends Encoder[String] :
  override def encode(schema: Schema): String => Any = string =>
    ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8))

/**
  * An [[Encoder]] for Strings that encodes as [[GenericFixed]]s.
  */
object FixedStringEncoder extends Encoder[String] :
  override def encode(schema: Schema): String => Any = string =>
    val bytes = string.getBytes(StandardCharsets.UTF_8)
    if (bytes.length > schema.getFixedSize)
      throw new Avro4sEncodingException(s"Cannot write string with ${bytes.length} bytes to fixed type of size ${schema.getFixedSize}")
    GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(bytes).array, schema).asInstanceOf[GenericData.Fixed]
