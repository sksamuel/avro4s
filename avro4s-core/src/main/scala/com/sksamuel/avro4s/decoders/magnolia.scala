package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Decoder, Encoder}
import magnolia.{CaseClass, AutoDerivation, SealedTrait}

trait MagnoliaDerivedDecoder extends AutoDerivation[Decoder] :
  override def join[T](ctx: CaseClass[Decoder, T]): Decoder[T] = new RecordDecoder(ctx)
  override def split[T](ctx: SealedTrait[Decoder, T]): Decoder[T] = ???