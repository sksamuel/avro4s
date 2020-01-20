package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate}
import magnolia.Subtype

object TypeUnionEntry {

  trait EntryDecoder[T] {
    def decodeSubtype(value: Any): T
  }

  trait EntryEncoder[T] {
    def encodeSubtype(value: T): AnyRef
  }

  trait EntryCodec[T] extends EntryDecoder[T] with EntryEncoder[T]

  class UnionEntryCodec[T](st: Subtype[Codec, T], update: SchemaUpdate)
      extends CodecBase[Codec, T](st, update)
      with EntryCodec[T] {

    def decodeSubtype(value: Any): T = decodeSubtype(typeclass, value)

    def encodeSubtype(value: T): AnyRef = encodeSubtype(typeclass, value)
  }

  class UnionEntryEncoder[T](st: Subtype[EncoderV2, T], update: SchemaUpdate)
      extends CodecBase[EncoderV2, T](st, update)
      with EntryEncoder[T] {

    def encodeSubtype(value: T): AnyRef = encodeSubtype(typeclass, value)
  }

  class UnionEntryDecoder[T](st: Subtype[DecoderV2, T], overrides: SchemaUpdate)
      extends CodecBase[DecoderV2, T](st, overrides)
      with EntryDecoder[T] {

    def decodeSubtype(value: Any): T = decodeSubtype(typeclass, value)
  }

  abstract class CodecBase[Typeclass[X] <: SchemaAware[Typeclass, X], T](val st: Subtype[Typeclass, T],
                                                                         update: SchemaUpdate) {

    protected val typeclass: Typeclass[st.SType] = {
      (st.typeclass, update) match {
        case (tc, FullSchemaUpdate(s))                                               => tc.withSchema(s.forType)
        case (a: NamespaceAware[Typeclass[st.SType]] @unchecked, NamespaceUpdate(n)) => a.withNamespace(n)
        case (tc, _)                                                                 => tc
      }
    }

    val fullName = typeclass.schema.getFullName

    val schema = typeclass.schema

    protected def encodeSubtype(encoder: EncoderV2[st.SType], value: T): AnyRef = encoder.encode(st.cast(value))

    protected def decodeSubtype(decoder: DecoderV2[st.SType], value: Any): st.SType = decoder.decode(value)
  }
}
