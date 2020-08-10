package com.sksamuel.avro4s

import magnolia.Subtype

private[avro4s] object TypeUnionEntry {

  class UnionEncoder[T](st: Subtype[Encoder, T]) {
    class SubtypeEncoder(encoder: Encoder[st.SType]) {
      val subtype = st
      val schema = encoder.schema

      def encodeSubtype(value: T): AnyRef = encoder.encode(st.cast(value))
    }

    def apply(env: DefinitionEnvironment[Encoder], update: SchemaUpdate) =
      new SubtypeEncoder(st.typeclass.resolveEncoder(env, update))
  }

  class UnionDecoder[T](st: Subtype[Decoder, T]) {
    class SubtypeDecoder(decoder: Decoder[st.SType]) {
      val subtype = st
      val schema = decoder.schema
      val fullName = schema.getFullName

      def decodeSubtype(value: AvroValue): T = decoder.decode(value)
    }

    def apply(env: DefinitionEnvironment[Decoder], update: SchemaUpdate) =
      new SubtypeDecoder(st.typeclass.resolveDecoder(env, update))
  }
}
