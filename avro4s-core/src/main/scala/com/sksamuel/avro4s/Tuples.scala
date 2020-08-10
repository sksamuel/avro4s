package com.sksamuel.avro4s

import com.sksamuel.avro4s.AvroValue.AvroRecord
import org.apache.avro.{Schema, SchemaBuilder}

trait TupleSchemaFors {
  import Tuples._

  implicit def tuple2SchemaFor[A, B](implicit a: SchemaFor[A], b: SchemaFor[B]): SchemaFor[(A, B)] =
    new ResolvableSchemaFor[(A, B)] {
      def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[(A, B)] =
        createTuple2SchemaFor(a.resolveSchemaFor(env, update), b.resolveSchemaFor(env, update))
    }

  implicit def tuple3SchemaFor[A, B, C](implicit a: SchemaFor[A],
                                        b: SchemaFor[B],
                                        c: SchemaFor[C]): SchemaFor[(A, B, C)] = new ResolvableSchemaFor[(A, B, C)] {
    def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[(A, B, C)] =
      createTuple3SchemaFor(a.resolveSchemaFor(env, update),
                            b.resolveSchemaFor(env, update),
                            c.resolveSchemaFor(env, update))
  }

  implicit def tuple4SchemaFor[A, B, C, D](implicit a: SchemaFor[A],
                                           b: SchemaFor[B],
                                           c: SchemaFor[C],
                                           d: SchemaFor[D]): SchemaFor[(A, B, C, D)] =
    new ResolvableSchemaFor[(A, B, C, D)] {
      def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[(A, B, C, D)] =
        createTuple4SchemaFor(a.resolveSchemaFor(env, update),
                              b.resolveSchemaFor(env, update),
                              c.resolveSchemaFor(env, update),
                              d.resolveSchemaFor(env, update))
    }

  implicit def tuple5SchemaFor[A, B, C, D, E](implicit a: SchemaFor[A],
                                              b: SchemaFor[B],
                                              c: SchemaFor[C],
                                              d: SchemaFor[D],
                                              e: SchemaFor[E]): SchemaFor[(A, B, C, D, E)] =
    new ResolvableSchemaFor[(A, B, C, D, E)] {
      def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[(A, B, C, D, E)] =
        createTuple5SchemaFor(
          a.resolveSchemaFor(env, update),
          b.resolveSchemaFor(env, update),
          c.resolveSchemaFor(env, update),
          d.resolveSchemaFor(env, update),
          e.resolveSchemaFor(env, update)
        )
    }
}

trait TupleEncoders {
  import Tuples._
  import com.sksamuel.avro4s.EncoderHelpers._

  implicit def tuple2Encoder[A, B](implicit a: Encoder[A], b: Encoder[B]): Encoder[(A, B)] =
    new ResolvableEncoder[(A, B)] {
      def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[(A, B)] = {
        val encoderA = a.resolveEncoder(env, mapTupleUpdate(0, update))
        val encoderB = b.resolveEncoder(env, mapTupleUpdate(1, update))

        new Encoder[(A, B)] {

          def schemaFor: SchemaFor[(A, B)] = createTuple2SchemaFor[A, B](encoderA.schemaFor, encoderB.schemaFor)

          def encode(value: (A, B)): AnyRef =
            ImmutableRecord(
              schema,
              Vector(encoderA.encode(value._1), encoderB.encode(value._2))
            )

          override def withSchema(schemaFor: SchemaFor[(A, B)]): Encoder[(A, B)] =
            buildWithSchema(tuple2Encoder(a, b), schemaFor)
        }
      }
    }

  implicit def tuple3Encoder[A, B, C](implicit a: Encoder[A], b: Encoder[B], c: Encoder[C]): Encoder[(A, B, C)] =
    new ResolvableEncoder[(A, B, C)] {
      def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[(A, B, C)] = {
        val encoderA = a.resolveEncoder(env, mapTupleUpdate(0, update))
        val encoderB = b.resolveEncoder(env, mapTupleUpdate(1, update))
        val encoderC = c.resolveEncoder(env, mapTupleUpdate(2, update))

        new Encoder[(A, B, C)] {

          def schemaFor: SchemaFor[(A, B, C)] =
            createTuple3SchemaFor[A, B, C](encoderA.schemaFor, encoderB.schemaFor, encoderC.schemaFor)

          def encode(value: (A, B, C)): AnyRef = ImmutableRecord(
            schema,
            Vector(encoderA.encode(value._1), encoderB.encode(value._2), encoderC.encode(value._3))
          )

          override def withSchema(schemaFor: SchemaFor[(A, B, C)]): Encoder[(A, B, C)] =
            buildWithSchema(tuple3Encoder(a, b, c), schemaFor)
        }
      }
    }

  implicit def tuple4Encoder[A, B, C, D](implicit a: Encoder[A],
                                         b: Encoder[B],
                                         c: Encoder[C],
                                         d: Encoder[D]): Encoder[(A, B, C, D)] = new ResolvableEncoder[(A, B, C, D)] {
    def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[(A, B, C, D)] = {
      val encoderA = a.resolveEncoder(env, mapTupleUpdate(0, update))
      val encoderB = b.resolveEncoder(env, mapTupleUpdate(1, update))
      val encoderC = c.resolveEncoder(env, mapTupleUpdate(2, update))
      val encoderD = d.resolveEncoder(env, mapTupleUpdate(3, update))

      new Encoder[(A, B, C, D)] {

        def schemaFor: SchemaFor[(A, B, C, D)] =
          createTuple4SchemaFor[A, B, C, D](encoderA.schemaFor,
                                            encoderB.schemaFor,
                                            encoderC.schemaFor,
                                            encoderD.schemaFor)

        def encode(value: (A, B, C, D)): AnyRef = ImmutableRecord(
          schema,
          Vector(encoderA.encode(value._1),
                 encoderB.encode(value._2),
                 encoderC.encode(value._3),
                 encoderD.encode(value._4))
        )

        override def withSchema(schemaFor: SchemaFor[(A, B, C, D)]): Encoder[(A, B, C, D)] =
          buildWithSchema(tuple4Encoder(a, b, c, d), schemaFor)
      }
    }
  }

  implicit def tuple5Encoder[A, B, C, D, E](implicit a: Encoder[A],
                                            b: Encoder[B],
                                            c: Encoder[C],
                                            d: Encoder[D],
                                            e: Encoder[E]): Encoder[(A, B, C, D, E)] =
    new ResolvableEncoder[(A, B, C, D, E)] {
      def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[(A, B, C, D, E)] = {
        val encoderA = a.resolveEncoder(env, mapTupleUpdate(0, update))
        val encoderB = b.resolveEncoder(env, mapTupleUpdate(1, update))
        val encoderC = c.resolveEncoder(env, mapTupleUpdate(2, update))
        val encoderD = d.resolveEncoder(env, mapTupleUpdate(3, update))
        val encoderE = e.resolveEncoder(env, mapTupleUpdate(4, update))

        new Encoder[(A, B, C, D, E)] {

          def schemaFor: SchemaFor[(A, B, C, D, E)] =
            createTuple5SchemaFor[A, B, C, D, E](encoderA.schemaFor,
                                                 encoderB.schemaFor,
                                                 encoderC.schemaFor,
                                                 encoderD.schemaFor,
                                                 encoderE.schemaFor)

          def encode(value: (A, B, C, D, E)): AnyRef = ImmutableRecord(
            schema,
            Vector(encoderA.encode(value._1),
                   encoderB.encode(value._2),
                   encoderC.encode(value._3),
                   encoderD.encode(value._4),
                   encoderE.encode(value._5))
          )

          override def withSchema(schemaFor: SchemaFor[(A, B, C, D, E)]): Encoder[(A, B, C, D, E)] =
            buildWithSchema(tuple5Encoder(a, b, c, d, e), schemaFor)
        }
      }
    }

}

trait TupleDecoders {
  import Tuples._
  import com.sksamuel.avro4s.DecoderHelpers._

  implicit def tuple2Decoder[A, B](implicit a: Decoder[A], b: Decoder[B]): Decoder[(A, B)] =
    new ResolvableDecoder[(A, B)] {
      def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[(A, B)] = {
        val decoderA = a.resolveDecoder(env, mapTupleUpdate(0, update))
        val decoderB = b.resolveDecoder(env, mapTupleUpdate(1, update))

        new Decoder[(A, B)] {

          def schemaFor: SchemaFor[(A, B)] = createTuple2SchemaFor[A, B](decoderA.schemaFor, decoderB.schemaFor)

          override def decode(value: AvroValue): (A, B) = value match {
            case AvroRecord(record) =>
              (
                decoderA.decode(AvroValue.unsafeFromAny(record.get("_1"))),
                decoderB.decode(AvroValue.unsafeFromAny(record.get("_2")))
              )
            case _ => throw Avro4sUnsupportedValueException(value, this)
          }

          override def withSchema(schemaFor: SchemaFor[(A, B)]): Decoder[(A, B)] =
            buildWithSchema(tuple2Decoder(a, b), schemaFor)
        }
      }
    }

  implicit def tuple3Decoder[A, B, C](implicit
                                      a: Decoder[A],
                                      b: Decoder[B],
                                      c: Decoder[C]): Decoder[(A, B, C)] = new ResolvableDecoder[(A, B, C)] {
    def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[(A, B, C)] = {
      val decoderA = a.resolveDecoder(env, mapTupleUpdate(0, update))
      val decoderB = b.resolveDecoder(env, mapTupleUpdate(1, update))
      val decoderC = c.resolveDecoder(env, mapTupleUpdate(2, update))

      new Decoder[(A, B, C)] {

        def schemaFor: SchemaFor[(A, B, C)] =
          createTuple3SchemaFor[A, B, C](decoderA.schemaFor, decoderB.schemaFor, decoderC.schemaFor)

        override def decode(value: AvroValue): (A, B, C) = value match {
          case AvroRecord(record) =>
            (
              decoderA.decode(AvroValue.unsafeFromAny(record.get("_1"))),
              decoderB.decode(AvroValue.unsafeFromAny(record.get("_2"))),
              decoderC.decode(AvroValue.unsafeFromAny(record.get("_3")))
            )
          case _ => throw Avro4sUnsupportedValueException(value, this)
        }

        override def withSchema(schemaFor: SchemaFor[(A, B, C)]): Decoder[(A, B, C)] =
          buildWithSchema(tuple3Decoder(a, b, c), schemaFor)
      }
    }
  }

  implicit def tuple4Decoder[A, B, C, D](implicit
                                         a: Decoder[A],
                                         b: Decoder[B],
                                         c: Decoder[C],
                                         d: Decoder[D]): Decoder[(A, B, C, D)] = new ResolvableDecoder[(A, B, C, D)] {
    def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[(A, B, C, D)] = {
      val decoderA = a.resolveDecoder(env, mapTupleUpdate(0, update))
      val decoderB = b.resolveDecoder(env, mapTupleUpdate(1, update))
      val decoderC = c.resolveDecoder(env, mapTupleUpdate(2, update))
      val decoderD = d.resolveDecoder(env, mapTupleUpdate(3, update))

      new Decoder[(A, B, C, D)] {

        def schemaFor: SchemaFor[(A, B, C, D)] =
          createTuple4SchemaFor[A, B, C, D](decoderA.schemaFor,
                                            decoderB.schemaFor,
                                            decoderC.schemaFor,
                                            decoderD.schemaFor)

        override def decode(value: AvroValue): (A, B, C, D) = value match {
          case AvroRecord(record) =>
            (
              decoderA.decode(AvroValue.unsafeFromAny(record.get("_1"))),
              decoderB.decode(AvroValue.unsafeFromAny(record.get("_2"))),
              decoderC.decode(AvroValue.unsafeFromAny(record.get("_3"))),
              decoderD.decode(AvroValue.unsafeFromAny(record.get("_4")))
            )
          case _ => throw Avro4sUnsupportedValueException(value, this)
        }

        override def withSchema(schemaFor: SchemaFor[(A, B, C, D)]): Decoder[(A, B, C, D)] =
          buildWithSchema(tuple4Decoder(a, b, c, d), schemaFor)
      }
    }
  }

  implicit def tuple5Decoder[A, B, C, D, E](implicit
                                            a: Decoder[A],
                                            b: Decoder[B],
                                            c: Decoder[C],
                                            d: Decoder[D],
                                            e: Decoder[E]): Decoder[(A, B, C, D, E)] =
    new ResolvableDecoder[(A, B, C, D, E)] {
      def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[(A, B, C, D, E)] = {
        val decoderA = a.resolveDecoder(env, mapTupleUpdate(0, update))
        val decoderB = b.resolveDecoder(env, mapTupleUpdate(1, update))
        val decoderC = c.resolveDecoder(env, mapTupleUpdate(2, update))
        val decoderD = d.resolveDecoder(env, mapTupleUpdate(3, update))
        val decoderE = e.resolveDecoder(env, mapTupleUpdate(4, update))

        new Decoder[(A, B, C, D, E)] {

          def schemaFor: SchemaFor[(A, B, C, D, E)] =
            createTuple5SchemaFor[A, B, C, D, E](decoderA.schemaFor,
                                                 decoderB.schemaFor,
                                                 decoderC.schemaFor,
                                                 decoderD.schemaFor,
                                                 decoderE.schemaFor)

          override def decode(value: AvroValue): (A, B, C, D, E) = value match {
            case AvroRecord(record) =>
              (
                decoderA.decode(AvroValue.unsafeFromAny(record.get("_1"))),
                decoderB.decode(AvroValue.unsafeFromAny(record.get("_2"))),
                decoderC.decode(AvroValue.unsafeFromAny(record.get("_3"))),
                decoderD.decode(AvroValue.unsafeFromAny(record.get("_4"))),
                decoderE.decode(AvroValue.unsafeFromAny(record.get("_5")))
              )
            case _ => throw Avro4sUnsupportedValueException(value, this)
          }

          override def withSchema(schemaFor: SchemaFor[(A, B, C, D, E)]): Decoder[(A, B, C, D, E)] =
            buildWithSchema(tuple5Decoder(a, b, c, d, e), schemaFor)
        }
      }
    }
}

object Tuples {

  def extractTupleSchema(pos: Int)(schema: Schema): Schema = {
    if(schema.getType != Schema.Type.RECORD)
      throw new Avro4sConfigurationException(s"Schema type for tuples must be RECORD, received ${schema.getType}")
    if(pos >= schema.getFields.size)
      throw new Avro4sConfigurationException(s"Record schema for tuple has only ${schema.getFields.size} fields, more are required")
    schema.getFields.get(pos).schema()
  }

  def mapTupleUpdate(pos: Int, update: SchemaUpdate): SchemaUpdate =
    EncoderHelpers.mapFullUpdate(extractTupleSchema(pos), update)

  def createTuple2SchemaFor[A, B](implicit a: SchemaFor[A], b: SchemaFor[B]): SchemaFor[(A, B)] = {
    // format: off
    SchemaFor(SchemaBuilder.record("Tuple2").namespace("scala").doc(null)
      .fields()
      .name("_1").`type`(a.schema).noDefault()
      .name("_2").`type`(b.schema).noDefault()
      .endRecord(),
      a.fieldMapper
    )
    // format: on
  }

  def createTuple3SchemaFor[A, B, C](implicit a: SchemaFor[A],
                                     b: SchemaFor[B],
                                     c: SchemaFor[C]): SchemaFor[(A, B, C)] = {
    // format: off
    SchemaFor(SchemaBuilder.record("Tuple3").namespace("scala").doc(null)
      .fields()
      .name("_1").`type`(a.schema).noDefault()
      .name("_2").`type`(b.schema).noDefault()
      .name("_3").`type`(c.schema).noDefault()
      .endRecord(),
      a.fieldMapper
    )
    // format: on
  }

  def createTuple4SchemaFor[A, B, C, D](implicit a: SchemaFor[A],
                                        b: SchemaFor[B],
                                        c: SchemaFor[C],
                                        d: SchemaFor[D]): SchemaFor[(A, B, C, D)] = {
    // format: off
    SchemaFor(SchemaBuilder.record("Tuple4").namespace("scala").doc(null)
      .fields()
      .name("_1").`type`(a.schema).noDefault()
      .name("_2").`type`(b.schema).noDefault()
      .name("_3").`type`(c.schema).noDefault()
      .name("_4").`type`(d.schema).noDefault()
      .endRecord(),
      a.fieldMapper
    )
    // format: on
  }

  def createTuple5SchemaFor[A, B, C, D, E](implicit a: SchemaFor[A],
                                           b: SchemaFor[B],
                                           c: SchemaFor[C],
                                           d: SchemaFor[D],
                                           e: SchemaFor[E]): SchemaFor[(A, B, C, D, E)] = {
    // format: off
    SchemaFor(SchemaBuilder.record("Tuple5").namespace("scala").doc(null)
      .fields()
      .name("_1").`type`(a.schema).noDefault()
      .name("_2").`type`(b.schema).noDefault()
      .name("_3").`type`(c.schema).noDefault()
      .name("_4").`type`(d.schema).noDefault()
      .name("_5").`type`(e.schema).noDefault()
      .endRecord(),
      a.fieldMapper
    )
    // format: on
  }
}
