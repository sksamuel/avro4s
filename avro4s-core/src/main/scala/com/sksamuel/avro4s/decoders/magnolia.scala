package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.typeutils.{CaseClassShape, DatatypeShape, SealedTraitShape}
import com.sksamuel.avro4s.{Decoder, Encoder}
import magnolia.{AutoDerivation, CaseClass, SealedTrait}

trait MagnoliaDerivedDecoder extends AutoDerivation[Decoder] :

  override def join[T](ctx: CaseClass[Decoder, T]): Decoder[T] =
    println("**" + ctx)
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record => new RecordDecoder(ctx)
      case CaseClassShape.ValueType => new RecordDecoder(ctx)
    }

  override def split[T](ctx: SealedTrait[Decoder, T]): Decoder[T] =
    DatatypeShape.of[T](ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.decoder(ctx)
      case SealedTraitShape.Enum => ??? // SealedTraits.encoder(ctx)
    }