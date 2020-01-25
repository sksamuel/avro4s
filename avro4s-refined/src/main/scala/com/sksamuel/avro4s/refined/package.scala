package com.sksamuel.avro4s

import eu.timepit.refined.api.{RefType, Validate}

package object refined {

  implicit def refinedSchemaFor[T: SchemaFor, P, F[_, _]]: SchemaFor[F[T, P]] =
    (fieldMapper: FieldMapper) => SchemaFor[T].schema(fieldMapper)

  implicit def refinedEncoder[T: Encoder, P, F[_, _] : RefType]: Encoder[F[T, P]] =
    Encoder[T].comap(RefType[F].unwrap)

  implicit def refinedDecoder[T: Decoder, P, F[_, _] : RefType](implicit validate: Validate[T, P]): Decoder[F[T, P]] =
    Decoder[T].map(RefType[F].refine[P].unsafeFrom[T])

  implicit def refinedSchemaForV2[T, P, F[_, _]](schemaFor: SchemaForV2[T]): SchemaForV2[F[T, P]] =
    schemaFor.forType

  implicit def refinedEncoderV2[T: EncoderV2, P, F[_, _] : RefType]: EncoderV2[F[T, P]] =
    EncoderV2[T].comap(RefType[F].unwrap)

  implicit def refinedDecoderV2[T: DecoderV2, P, F[_, _] : RefType](implicit validate: Validate[T, P]): DecoderV2[F[T, P]] =
    DecoderV2[T].map(RefType[F].refine[P].unsafeFrom[T])

  implicit def refinedCodec[T: Codec, P, F[_, _] : RefType](implicit validate: Validate[T, P]): Codec[F[T, P]] =
    Codec[T].inmap(RefType[F].refine[P].unsafeFrom[T], RefType[F].unwrap)
}
