package com.sksamuel.avro4s

import eu.timepit.refined.api.{RefType, Validate}

package object refined {

  implicit def refinedSchemaFor[T, P, F[_, _]](implicit schemaFor: SchemaFor[T]): SchemaFor[F[T, P]] =
    (namingStrategy: NamingStrategy) => schemaFor.schema(namingStrategy)

  implicit def refinedEncoder[T, P, F[_, _]](implicit encoder: Encoder[T],
                                             refType: RefType[F]): Encoder[F[T, P]] =
    encoder.comap(refType.unwrap)

  implicit def refinedDecoder[T, P, F[_, _]](implicit decoder: Decoder[T],
                                             validate: Validate[T, P],
                                             refType: RefType[F]): Decoder[F[T, P]] =
    decoder.map(refType.refine[P].unsafeFrom[T])
}
