package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.typeutils.{CaseClassShape, DatatypeShape, SealedTraitShape}
import com.sksamuel.avro4s.{Encoder, SchemaConfiguration, SchemaFor}
import magnolia.{CaseClass, Derivation, SealedTrait, TypeInfo}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.deriving.Mirror

trait MagnoliaDerivedEncoder extends Derivation[Encoder] :
  override def join[T](ctx: CaseClass[Encoder, T]): Encoder[T] = new RecordEncoder(ctx)
  override def split[T](ctx: SealedTrait[Encoder, T]): Encoder[T] = ???