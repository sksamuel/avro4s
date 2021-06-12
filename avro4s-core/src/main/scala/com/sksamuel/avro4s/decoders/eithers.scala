package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Avro4sDecodingException, Decoder, TypeGuardedDecoding}
import com.sksamuel.avro4s.avroutils.SchemaHelper
import org.apache.avro.Schema

trait EitherDecoders:
  given eitherDecoder[A, B](using leftDecoder: Decoder[A],
                            leftGuard: TypeGuardedDecoding[A],
                            rightDecoder: Decoder[B],
                            rightGuard: TypeGuardedDecoding[B]): Decoder[Either[A, B]] = new Decoder[Either[A, B]] {

    override def decode(schema: Schema): Any => Either[A, B] = {
      require(schema.isUnion)

      val leftSchema = SchemaHelper.extractEitherLeftSchema(schema)
      val rightSchema = SchemaHelper.extractEitherRightSchema(schema)

      val leftDecode = leftDecoder.decode(leftSchema)
      val rightDecode = rightDecoder.decode(rightSchema)

      val leftP: PartialFunction[Any, Boolean] = leftGuard.guard(leftSchema)
      val rightP: PartialFunction[Any, Boolean] = rightGuard.guard(rightSchema)

      // how do we know whether the incoming value should be decoded to a a left or a right ?
      // we can compare types for primitives, and if a record we can compare schemas
      { value =>
        if (leftP.isDefinedAt(value)) {
          Left(leftDecode(value))
        } else if (rightP.isDefinedAt(value)) {
          Right(rightDecode(value))
        } else {
          val nameA = leftSchema.getFullName
          val nameB = rightSchema.getFullName
          throw new Avro4sDecodingException(s"Could not decode $value into Either[$nameA, $nameB]", value)
        }
      }
    }
  }