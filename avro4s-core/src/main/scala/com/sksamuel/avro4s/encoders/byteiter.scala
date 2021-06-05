package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.Encoder
import org.apache.avro.Schema

import java.nio.ByteBuffer

trait ByteIterableEncoders:
  given Encoder[ByteBuffer] = ByteBufferEncoder
  given Encoder[Array[Byte]] = ByteArrayEncoder
  given Encoder[List[Byte]] = IterableByteEncoder.contramap(_.toIterable)
  given Encoder[Vector[Byte]] = IterableByteEncoder.contramap(_.toIterable)
  given Encoder[Seq[Byte]] = IterableByteEncoder.contramap(_.toIterable)

object ByteBufferEncoder extends Encoder[ByteBuffer] :
  override def encode(schema: Schema): ByteBuffer => Any = Encoder.identity[ByteBuffer].encode(schema)

object ByteArrayEncoder extends Encoder[Array[Byte]] :
  override def encode(schema: Schema): Array[Byte] => Any = { bytes => ByteBuffer.wrap(bytes) }

object IterableByteEncoder extends Encoder[Iterable[Byte]] :
  override def encode(schema: Schema): Iterable[Byte] => Any = { bytes => ByteBuffer.wrap(bytes.toArray) }



//  private sealed trait ByteArrayEncoderBase extends Encoder[Array[Byte]] {
//    override def withSchema(schemaFor: SchemaFor[Array[Byte]]): Encoder[Array[Byte]] =
//      schemaFor.schema.getType match {
//        case Schema.Type.BYTES => ByteArrayEncoder
//        case Schema.Type.FIXED => new FixedByteArrayEncoder(schemaFor)
//        case _ =>
//          throw new Avro4sConfigurationException(
//            s"Byte array codec doesn't support schema type ${schemaFor.schema.getType}")
//      }
//  }
