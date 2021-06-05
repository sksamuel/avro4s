//package com.sksamuel.avro4s
//
//import java.nio.ByteBuffer
//
//import org.apache.avro.{Schema, SchemaBuilder}
//import org.apache.avro.generic.{GenericData, GenericFixed}
//
//trait ByteIterableDecoders {
//
//  implicit val ByteArrayDecoder: Decoder[Array[Byte]] = new ByteArrayDecoderBase {
//    val schemaFor = SchemaFor[Array[Byte]](SchemaBuilder.builder().bytesType())
//  }
//
//  implicit val ByteListDecoder: Decoder[List[Byte]] = iterableByteDecoder(_.toList)
//  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = iterableByteDecoder(_.toVector)
//  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = iterableByteDecoder(_.toSeq)
//
//  private def iterableByteDecoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte]): Decoder[C[Byte]] =
//    new IterableByteDecoder[C](build)
//
//  private sealed trait ByteArrayDecoderBase extends Decoder[Array[Byte]] {
//
//    def decode(value: Any): Array[Byte] = value match {
//      case buffer: ByteBuffer  => buffer.array
//      case array: Array[Byte]  => array
//      case fixed: GenericFixed => fixed.bytes
//      case _                   => throw new Avro4sDecodingException(s"Byte array decoder cannot decode '$value'", value, this)
//    }
//
//    override def withSchema(schemaFor: SchemaFor[Array[Byte]]): Decoder[Array[Byte]] =
//      schemaFor.schema.getType match {
//        case Schema.Type.BYTES => ByteArrayDecoder
//        case Schema.Type.FIXED => new FixedByteArrayDecoder(schemaFor)
//        case _ =>
//          throw new Avro4sConfigurationException(
//            s"Byte array decoder doesn't support schema type ${schemaFor.schema.getType}")
//      }
//  }
//
//  private class FixedByteArrayDecoder(val schemaFor: SchemaFor[Array[Byte]]) extends ByteArrayDecoderBase {
//    if (schema.getType != Schema.Type.FIXED)
//      throw new Avro4sConfigurationException(s"Fixed byte array decoder only supports schema type FIXED, got $schema")
//  }
//
//  private class IterableByteDecoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte],
//                                                         byteArrayDecoder: Decoder[Array[Byte]] = ByteArrayDecoder)
//      extends Decoder[C[Byte]] {
//
//    val schemaFor: SchemaFor[C[Byte]] = byteArrayDecoder.schemaFor.forType
//
//    def decode(value: Any): C[Byte] = build(byteArrayDecoder.decode(value))
//
//    override def withSchema(schemaFor: SchemaFor[C[Byte]]): Decoder[C[Byte]] =
//      new IterableByteDecoder(build, byteArrayDecoder.withSchema(schemaFor.map(identity)))
//  }
//}
//
//trait ByteIterableEncoders {
//

//
//  private class FixedByteArrayEncoder(val schemaFor: SchemaFor[Array[Byte]]) extends ByteArrayEncoderBase {
//    if (schema.getType != Schema.Type.FIXED)
//      throw new Avro4sConfigurationException(s"Fixed byte array encoder only supports schema type FIXED, got $schema")
//
//    def encode(value: Array[Byte]): AnyRef = {
//      val array = new Array[Byte](schema.getFixedSize)
//      System.arraycopy(value, 0, array, 0, value.length)
//      GenericData.get.createFixed(null, array, schema)
//    }
//  }

//}
