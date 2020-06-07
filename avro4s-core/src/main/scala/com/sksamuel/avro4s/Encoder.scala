package com.sksamuel.avro4s

import com.sksamuel.avro4s.Encoder.DelegatingEncoder
import com.sksamuel.avro4s.EncoderHelpers.FieldEncoder
import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NoUpdate}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.reflect.runtime.universe.WeakTypeTag
import scala.language.experimental.macros
import scala.collection.JavaConverters._

/**
  * An [[Encoder]] encodes a Scala value of type T into a compatible
  * Avro value based on the given schema.
  *
  * For example, given a string, and a schema of type Schema.Type.STRING
  * then the string would be encoded as an instance of Utf8, whereas
  * the same string and a Schema.Type.FIXED would be encoded as an
  * instance of GenericFixed.
  *
  * Another example is given a Scala enumeration value, and a schema of
  * type Schema.Type.ENUM, the value would be encoded as an instance
  * of GenericData.EnumSymbol.
  */
trait Encoder[T] extends SchemaAware[Encoder, T] with Serializable { self =>

  /**
    * Encodes the given value to a value supported by Avro's generic data model
    */
  def encode(value: T): AnyRef

  /**
    * Creates a variant of this Encoder using the given schema (e.g. to use a fixed schema for byte arrays instead of
    * the default bytes schema)
    *
    * @param schemaFor the schema to use
    */
  def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    val sf = schemaFor
    new Encoder[T] {
      val schemaFor: SchemaFor[T] = sf

      def encode(value: T): AnyRef = self.encode(value)
    }
  }

  /**
    * produces an Encoder that is guaranteed to be resolved and ready to be used.
    *
    * This is necessary for properly setting up Encoder instances for recursive types.
    */
  def resolveEncoder(): Encoder[T] = resolveEncoder(DefinitionEnvironment.empty, NoUpdate)

  /**
    * For advanced use only to properly setup Encoder instances for recursive types.
    *
    * Resolves the Encoder with the provided environment, and (potentially) pushes down overrides from annotations on
    * sealed traits to case classes, or from annotations on parameters to types.
    *
    * @param env definition environment containing already defined record encoders
    * @param update schema changes to apply
    */
  def resolveEncoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[T] = (self, update) match {
    case (resolvable: ResolvableEncoder[T], _) => resolvable.encoder(env, update)
    case (_, FullSchemaUpdate(sf))             => self.withSchema(sf.forType)
    case _                                     => self
  }

  def comap[S](f: S => T): Encoder[S] = new DelegatingEncoder(this, this.schemaFor.forType, f)
}

/**
  * An Encoder that needs to be resolved before it is usable. Resolution is needed to properly setup Encoder instances
  * for recursive types.
  *
  * If this instance is used without resolution, it falls back to use an adhoc-resolved instance and delegates all
  * operations to it. This involves a performance penalty of lazy val access that can be avoided by
  * calling [[Encoder.resolveEncoder]] and using that.
  *
  * For examples on how to define custom ResolvableEncoder instances, see the Readme and RecursiveEncoderTest.
  *
  * @tparam T type this encoder is for (primitive type, case class, sealed trait, or enum e.g.).
  */
trait ResolvableEncoder[T] extends Encoder[T] {

  /**
    * Creates an Encoder instance (and applies schema changes given) or returns an already existing value from the
    * given definition environment.
    *
    * @param env    definition environment to use
    * @param update schema update to apply
    * @return either an already existing value from env or a new created instance.
    */
  def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[T]

  lazy val adhocInstance = encoder(DefinitionEnvironment.empty, NoUpdate)

  def encode(value: T): AnyRef = adhocInstance.encode(value)

  def schemaFor: SchemaFor[T] = adhocInstance.schemaFor

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = adhocInstance.withSchema(schemaFor)
}

object Encoder
    extends MagnoliaDerivedEncoders
    with ShapelessCoproductEncoders
    with CollectionAndContainerEncoders
    with ByteIterableEncoders
    with BigDecimalEncoders
    with TupleEncoders
    with TemporalEncoders
    with BaseEncoders {

  def apply[T](implicit encoder: Encoder[T]): Encoder[T] = encoder

  private class DelegatingEncoder[T, S](encoder: Encoder[T], val schemaFor: SchemaFor[S], comap: S => T)
    extends Encoder[S] {

    def encode(value: S): AnyRef = encoder.encode(comap(value))

    override def withSchema(schemaFor: SchemaFor[S]): Encoder[S] = {
      // pass through schema so that underlying encoder performs desired transformations.
      val modifiedEncoder = encoder.withSchema(schemaFor.forType)
      new DelegatingEncoder[T, S](modifiedEncoder, schemaFor.forType, comap)
    }
  }

  def field[T, F](name: String, extractor: T => F)(implicit encoder: Encoder[F]): EncoderField[T, F] =
    EncoderField(name, extractor, encoder)

  def record[T: WeakTypeTag](name: String,
                namespace: String,
                doc: Option[String] = None,
                aliases: Seq[String] = Seq.empty,
                props: Map[String, String] = Map.empty)
               (fn: => List[EncoderField[T, _]]): Encoder[T] = new ResolvableEncoder[T] {
    def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[T] =
      env.get[T].getOrElse {
        // we could actually pick apart a schema update if that's a desired feature.
        require(update == NoUpdate, "Custom record encoders don't support .withSchema modifications")
        val recordSchema = Schema.createRecord(name, doc.orNull, namespace, false)
        aliases.foreach(recordSchema.addAlias)
        props.foreach { case (key, value) => schema.addProp(key, value) }
        var fieldEncoders = Seq.empty[FieldEncoder[T, _]]

        val encoder = new Encoder[T] {
          override val schemaFor: SchemaFor[T] = SchemaFor(recordSchema)

          override def encode(value: T): AnyRef = {
            val fields = new Array[AnyRef](fieldEncoders.size)
            var i = 0
            while(i < fields.length) {
              fields(i) = fieldEncoders(i).encodeField(value)
              i += 1
            }
            ImmutableRecord(schemaFor.schema, fields)
          }
        }

        val extendedEnv = env.updated(encoder)

        val fieldsWithEncoder = fn.map { field =>
          val fieldEncoder = field.resolve(extendedEnv)
          val schemaField = new Schema.Field(field.name, fieldEncoder.schema)
          schemaField -> fieldEncoder
        }

        recordSchema.setFields(fieldsWithEncoder.map(_._1).asJava)
        fieldEncoders = fieldsWithEncoder.map(_._2).toArray[FieldEncoder[T, _]]

        encoder
      }
  }

  def enum[T](name: String,
              namespace: String,
              doc: Option[String] = None,
              aliases: Seq[String] = Seq.empty,
              props: Map[String, String] = Map.empty,
              symbols: List[String])
             (encodeFn: T => String): Encoder[T] = {

    val builder = SchemaBuilder.enumeration(name).namespace(namespace)
    props.foreach { case (key, value) => builder.prop(key, value) }
    if (aliases.nonEmpty) builder.aliases(aliases: _*)
    doc.foreach(builder.doc)
    val enumSchema = builder.symbols(symbols: _*)

    new Encoder[T] {
      override def schemaFor: SchemaFor[T] = SchemaFor(enumSchema)
      override def encode(value: T): AnyRef = {
        val symbol = encodeFn(value)
        new GenericData.EnumSymbol(enumSchema, symbol)
      }
    }
  }
}

object EncoderHelpers {
  def buildWithSchema[T](encoder: Encoder[T], schemaFor: SchemaFor[T]): Encoder[T] =
    encoder.resolveEncoder(DefinitionEnvironment.empty, FullSchemaUpdate(schemaFor))

  def mapFullUpdate(f: Schema => Schema, update: SchemaUpdate) = update match {
    case full: FullSchemaUpdate => FullSchemaUpdate(SchemaFor(f(full.schemaFor.schema), full.schemaFor.fieldMapper))
    case _ => update
  }

  class FieldEncoder[T, F](extractor: T => F, encoder: Encoder[F]) {
    def encodeField(value: T): AnyRef = encoder.encode(extractor(value))

    def schema = encoder.schemaFor.schema
  }
}

case class EncoderField[T, F](name: String, extractor: T => F, encoder: Encoder[F]) {
  def resolve(env: DefinitionEnvironment[Encoder]): FieldEncoder[T, F] =
    new FieldEncoder[T, F](extractor, encoder.resolveEncoder(env, NoUpdate))
}