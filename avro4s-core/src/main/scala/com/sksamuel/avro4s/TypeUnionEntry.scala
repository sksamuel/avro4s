package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate}
import com.sksamuel.avro4s.TypeUnions.{EntryCodec, EntryDecoder, EntryEncoder}
import magnolia.Subtype

private[avro4s] object TypeUnionEntry {

  class UnionEntryCodec[T](st: Subtype[Codec, T], update: SchemaUpdate)
      extends EntryBase[Codec, T](st, update)
      with EntryCodec[T] {

    def decodeSubtype(value: Any): T = decodeSubtype(typeclass, value)

    def encodeSubtype(value: T): AnyRef = encodeSubtype(typeclass, value)
  }

  class UnionEntryEncoder[T](st: Subtype[Encoder, T], update: SchemaUpdate)
      extends EntryBase[Encoder, T](st, update)
      with EntryEncoder[T] {

    def encodeSubtype(value: T): AnyRef = encodeSubtype(typeclass, value)
  }

  class UnionEntryDecoder[T](st: Subtype[Decoder, T], overrides: SchemaUpdate)
      extends EntryBase[Decoder, T](st, overrides)
      with EntryDecoder[T] {

    def decodeSubtype(value: Any): T = decodeSubtype(typeclass, value)
  }

  abstract class EntryBase[Typeclass[X] <: SchemaAware[Typeclass, X], T](val st: Subtype[Typeclass, T],
                                                                         update: SchemaUpdate) {

    protected val typeclass: Typeclass[st.SType] = {
      (st.typeclass, update) match {
        case (tc, FullSchemaUpdate(s))                                                  => tc.withSchema(s.forType)
        case (a: NamespaceAware[Typeclass[st.SType]] @unchecked, NamespaceUpdate(n, _)) => a.withNamespace(n)
        case (tc, _)                                                                    => tc
      }
    }

    val fullName = typeclass.schema.getFullName

    val schema = typeclass.schema

    @inline
    protected final def encodeSubtype(encoder: Encoder[st.SType], value: T): AnyRef = encoder.encode(st.cast(value))

    @inline
    protected final def decodeSubtype(decoder: Decoder[st.SType], value: Any): st.SType = decoder.decode(value)
  }
}