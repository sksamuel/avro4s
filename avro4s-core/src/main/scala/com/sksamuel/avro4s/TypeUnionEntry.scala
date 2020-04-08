package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate}
import com.sksamuel.avro4s.TypeUnions.{EntryDecoder, EntryEncoder}
import magnolia.Subtype

private[avro4s] object TypeUnionEntry {

  class UnionEntryEncoder[T](st: Subtype[Encoder, T], update: SchemaUpdate)
      extends EntryBase[Encoder, T](st, update)
      with EntryEncoder[T] {

    def encodeSubtype(value: T): AnyRef = typeclass.encode(subtype.cast(value))
  }

  class UnionEntryDecoder[T](st: Subtype[Decoder, T], overrides: SchemaUpdate)
      extends EntryBase[Decoder, T](st, overrides)
      with EntryDecoder[T] {

    def decodeSubtype(value: Any): T = typeclass.decode(value)
  }

  abstract class EntryBase[Typeclass[X] <: SchemaAware[Typeclass, X], T](val subtype: Subtype[Typeclass, T],
                                                                         update: SchemaUpdate) {

    protected val typeclass: Typeclass[subtype.SType] = {
      (subtype.typeclass, update) match {
        case (tc, FullSchemaUpdate(s))                                                       => tc.withSchema(s.forType)
        case (a: NamespaceAware[Typeclass[subtype.SType]] @unchecked, NamespaceUpdate(n, _)) => a.withNamespace(n)
        case (tc, _)                                                                         => tc
      }
    }

    val fullName = typeclass.schema.getFullName

    val schema = typeclass.schema
  }
}
