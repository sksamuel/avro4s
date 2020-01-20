package com.sksamuel.avro4s

import java.time.Instant

import org.apache.avro.{LogicalTypes, SchemaBuilder}

trait TemporalCodecs {

  val InstantCodec: Codec[Instant] = Temporals.InstantCodec

}

trait TemporalEncoders {

  val InstantEncoder: EncoderV2[Instant] = Temporals.InstantCodec
}

trait TemporalDecoders {

  val InstantDecoder: DecoderV2[Instant] = Temporals.InstantCodec

}

object Temporals {

  val InstantSchema = SchemaForV2[Instant](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))

  val InstantCodec = BaseTypes.LongCodec.inmap[Instant](Instant.ofEpochMilli, _.toEpochMilli).withSchema(InstantSchema)

}
