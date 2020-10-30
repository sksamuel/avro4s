package com.sksamuel.avro4s

import eu.timepit.refined.api.{RefType, Validate}

import scala.reflect.runtime.universe._

package object refined {

  implicit def refinedSchemaFor[T, P, F[_, _] : RefType](implicit schemaFor: SchemaFor[T]): SchemaFor[F[T, P]] =
    schemaFor.forType

  implicit def refinedEncoder[T: Encoder, P, F[_, _] : RefType]: Encoder[F[T, P]] =
    Encoder[T].comap(RefType[F].unwrap)

  implicit def refinedDecoder[T: Decoder, P, F[_, _] : RefType](implicit validate: Validate[T, P]): Decoder[F[T, P]] =
    Decoder[T].map(RefType[F].refine[P].unsafeFrom[T])

  implicit def refinedTypeGuardedDecoding[T: WeakTypeTag, P, F[_, _]: RefType]: TypeGuardedDecoding[F[T, P]] = new TypeGuardedDecoding[F[T, P]] {
    override final def guard(decoderT: Decoder[F[T, P]]): PartialFunction[Any, F[T, P]] =
      TypeGuardedDecoding[T].guard(decoderT.map(RefType[F].unwrap)).andThen(RefType[F].unsafeWrap(_))
  }
}
