package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Decoder, Encoder}
import com.sksamuel.avro4s.encoders.RecordEncoder
import magnolia.{CaseClass, Derivation, SealedTrait}

trait MagnoliaDerivedDecoder extends Derivation[Decoder] :
  override def join[T](ctx: CaseClass[Decoder, T]): Decoder[T] = new RecordDecoder(ctx)
  override def split[T](ctx: SealedTrait[Decoder, T]): Decoder[T] = ???