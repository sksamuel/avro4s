package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.avroutils.SchemaHelper
import org.apache.avro.Schema

trait EitherEncoders:
  given eitherEncoder[A, B] (using a: Encoder[A], b: Encoder[B]): Encoder[Either[A, B]] with
    override def encode(schema: Schema): Either[A, B] => AnyRef = {
      require(schema.isUnion)

      val leftSchema = SchemaHelper.extractEitherLeftSchema(schema)
      val rightSchema = SchemaHelper.extractEitherRightSchema(schema)

      val encodeA = a.encode(leftSchema)
      val encodeB = b.encode(rightSchema)

      { value =>
        value match {
          case Left(l) => encodeA(l)
          case Right(r) => encodeB(r)
        }
      }
    }
