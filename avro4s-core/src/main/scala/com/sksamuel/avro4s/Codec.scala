package com.sksamuel.avro4s

import org.apache.avro.Schema

/**
  * Convenience trait that allows to define both custom [[Encoder]] and custom [[Decoder]] in one go, if both are needed.
  * This way, they don't have to be declared in two separate classes or objects.
  *
  * A codec can both encode a Scala value of type T into a compatible Avro value based on the given schema as well as
  * decode an Avro value, such as a GenericRecord, SpecificRecord, GenericFixed, EnumSymbol, or a basic JVM type, into a
  * target Scala type.
  */
trait Codec[T] extends Encoder[T] with Decoder[T]

//object Codec {
//
//  /**
//   * Enables decorating/enhancing a codec with two transformation functions
//   */
//  implicit class CodecOps[T](val codec: Codec[T]) extends AnyVal {
//    def inmap[S](map: T => S, comap: S => T): Decoder[S] = new DelegatingCodec(codec, codec.schemaFor.forType, map, comap)
//  }
//
//  private class DelegatingCodec[T, S](codec: Codec[T], val schemaFor: SchemaFor[S], map: T => S, comap: S => T)
//    extends Codec[S] {
//
//    def encode(value: S): AnyRef = codec.encode(comap(value))
//
//    def decode(value: Any): S = map(codec.decode(value))
//
//    override def withSchema(schemaFor: SchemaFor[S]): Codec[S] = {
//      // pass through schema so that underlying decoder performs desired transformations.
//      val modifiedCodec = codec.withSchema(schemaFor.forType)
//      new DelegatingCodec[T, S](modifiedCodec, schemaFor.forType, map, comap)
//    }
//  }
//}