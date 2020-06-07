package com.sksamuel.avro4s

import com.sksamuel.avro4s.Decoder.DelegatingDecoder
import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NoUpdate}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
  * A [[Decoder]] is used to convert an Avro value, such as a GenericRecord,
  * SpecificRecord, GenericFixed, EnumSymbol, or a basic JVM type, into a
  * target Scala type.
  *
  * For example, a Decoder[String] would convert an input of type Utf8 -
  * which is one of the ways Avro can encode strings - into a plain Java String.
  *
  * Another example, a decoder for Option[String] would handle inputs of null
  * by emitting a None, and a non-null input by emitting the decoded value
  * wrapped in a Some.
  *
  * A final example is converting a GenericData.Array or a Java collection type
  * into a Scala collection type.
  */
trait Decoder[T] extends SchemaAware[Decoder, T] with Serializable { self =>

  /**
    * Decodes the given value to an instance of T if possible.
    * Otherwise throw an error.
    */
  def decode(value: Any): T

  /**
    * Transforms the decoded value to create a decoder for type S
    */
  def map[S](f: T => S): Decoder[S] = new DelegatingDecoder(this, schemaFor.forType, f)

  /**
    * Creates a variant of this Decoder using the given schema (e.g. to use a fixed schema for byte arrays instead of
    * the default bytes schema)
    *
    * @param schemaFor the schema to use
    */
  def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    val sf = schemaFor
    new Decoder[T] {
      def schemaFor: SchemaFor[T] = sf
      def decode(value: Any): T = self.decode(value)
    }
  }

  /**
    * produces a Decoder that is guaranteed to be resolved and ready to be used.
    *
    * This is necessary for properly setting up Decoder instances for recursive types.
    */
  def resolveDecoder(): Decoder[T] = resolveDecoder(DefinitionEnvironment.empty, NoUpdate)

  /**
    * For advanced use only to properly setup Decoder instances for recursive types.
    *
    * Resolves the Decoder with the provided environment, and (potentially) pushes down overrides from annotations on
    * sealed traits to case classes, or from annotations on parameters to types.
    *
    * @param env definition environment containing already defined record encoders
    * @param update schema changes to apply
    */
  def resolveDecoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[T] = (self, update) match {
    case (resolvable: ResolvableDecoder[T], _) => resolvable.decoder(env, update)
    case (_, FullSchemaUpdate(sf))             => self.withSchema(sf.forType)
    case _                                     => self
  }

}

/**
  * A Decoder that needs to be resolved before it is usable. Resolution is needed to properly setup Decoder instances
  * for recursive types.
  *
  * If this instance is used without resolution, it falls back to use an adhoc-resolved instance and delegates all
  * operations to it. This involves a performance penalty of lazy val access that can be avoided by
  * calling [[Encoder.resolveEncoder]] and using that.
  *
  * For examples on how to define custom ResolvableDecoder instances, see the Readme and RecursiveDecoderTest.
  *
  * @tparam T type this encoder is for (primitive type, case class, sealed trait, or enum e.g.).
  */
trait ResolvableDecoder[T] extends Decoder[T] {

  /**
    * Creates a Decoder instance (and applies schema changes given) or returns an already existing value from the
    * given definition environment.
    *
    * @param env definition environment to use
    * @param update schema update to apply
    * @return either an already existing value from env or a new created instance.
    */
  def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[T]

  lazy val adhocInstance = decoder(DefinitionEnvironment.empty, NoUpdate)

  def decode(value: Any): T = adhocInstance.decode(value)

  def schemaFor: SchemaFor[T] = adhocInstance.schemaFor

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = adhocInstance.withSchema(schemaFor)
}

trait DecoderField[T] {
  def decodeField(record: GenericRecord): T

  def schemaField: Schema.Field

  def resolve(env: DefinitionEnvironment[Decoder]): DecoderField[T] = this
}

class SimpleDecoderField[T](name: String, decoder: Decoder[T]) extends DecoderField[T] {

  def decodeField(record: GenericRecord): T = decoder.decode(record.get(name))

  def schemaField = new Schema.Field(name, decoder.schema)

  def map[S](f: T => S): SimpleDecoderField[S] = new SimpleDecoderField[S](name, decoder.map(f))

  override def resolve(env: DefinitionEnvironment[Decoder]): DecoderField[T] =
    new SimpleDecoderField[T](name, decoder.resolveDecoder(env, NoUpdate))
}

class DecoderBuilder[T](val name: String,
                        val namespace: String,
                        val doc: Option[String],
                        val aliases: Seq[String],
                        val props: Map[String, String]) {
  def apply(fields: DecoderField[_]*)(constructor: Any): Decoder[T] = macro DecoderBuilder.record_impl[T]
}

object DecoderBuilder {

  type BuilderContext[T] = blackbox.Context { type PrefixType = DecoderBuilder[T] }

  def record_impl[T: c.WeakTypeTag](c: BuilderContext[T])(fields: c.Tree*)(constructor: c.Tree): c.Tree = {
    import c.universe._

    val decoderNames = fields.map(_ => TermName(c.freshName("decoder")))
    val cons = TermName(c.freshName("cons"))

    val decoderDeclarations = decoderNames.zip(fields).map {
      case (name, decoder) =>
        val typeArg = decoder.tpe.typeArgs.head
        q"var $name: com.sksamuel.avro4s.DecoderField[$typeArg] = $decoder"
    }

    val resolveDecoders = decoderNames.map(name => q"$name = $name.resolve(extendedEnv)")
    val schemaFields = decoderNames.map(name => q"$name.schemaField")
    val decodeFields = decoderNames.map(name => q"$name.decodeField(record)")

    val TParam = weakTypeOf[T]

    q"""{
      val $cons = $constructor

      ..$decoderDeclarations

      import org.apache.avro.{Schema, SchemaBuilder}
      import org.apache.avro.generic.GenericRecord
      import com.sksamuel.avro4s._
      import scala.collection.JavaConverters._

      new ResolvableDecoder[$TParam] {
        def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[$TParam] = env.get[$TParam].getOrElse {
          require(update == SchemaUpdate.NoUpdate, "Custom record decoders don't support .withSchema modifications")
          val recordSchema = Schema.createRecord(${c.prefix}.name, ${c.prefix}.doc.orNull, ${c.prefix}.namespace, false)
          ${c.prefix}.aliases.foreach(recordSchema.addAlias)
          ${c.prefix}.props.foreach { case (key, value) => schema.addProp(key, value) }

          val decoder = new Decoder[$TParam] {
            val schemaFor: SchemaFor[$TParam] = SchemaFor(recordSchema)

            def decode(value: Any): $TParam = value match {
              case record: GenericRecord => $cons(..$decodeFields)
              case _ => sys.error("This decoder can only handle GenericRecord [was " + value.getClass + "]")
            }
          }

          val extendedEnv = env.updated(decoder)
          ..$resolveDecoders
          recordSchema.setFields(List(..$schemaFields).asJava)
          decoder
        }
      }
    }"""
  }
}

object Decoder
    extends MagnoliaDerivedDecoders
    with ShapelessCoproductDecoders
    with CollectionAndContainerDecoders
    with TupleDecoders
    with ByteIterableDecoders
    with BigDecimalDecoders
    with TemporalDecoders
    with BaseDecoders {

  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder

  def field[T](name: String)(implicit decoder: Decoder[T]): SimpleDecoderField[T] = new SimpleDecoderField[T](name, decoder)

  def record[T](name: String,
                namespace: String,
                doc: Option[String] = None,
                aliases: Seq[String] = Seq.empty,
                props: Map[String, String] = Map.empty): DecoderBuilder[T] =
    new DecoderBuilder[T](name, namespace, doc, aliases, props)

  private class DelegatingDecoder[T, S](decoder: Decoder[T], val schemaFor: SchemaFor[S], map: T => S)
      extends Decoder[S] {

    def decode(value: Any): S = map(decoder.decode(value))

    override def withSchema(schemaFor: SchemaFor[S]): Decoder[S] = {
      // pass through schema so that underlying decoder performs desired transformations.
      val modifiedDecoder = decoder.withSchema(schemaFor.forType)
      new DelegatingDecoder[T, S](modifiedDecoder, schemaFor.forType, map)
    }
  }
}

object DecoderHelpers {
  def buildWithSchema[T](decoder: Decoder[T], schemaFor: SchemaFor[T]): Decoder[T] =
    decoder.resolveDecoder(DefinitionEnvironment.empty, FullSchemaUpdate(schemaFor))

  def mapFullUpdate(f: Schema => Schema, update: SchemaUpdate) = update match {
    case full: FullSchemaUpdate => FullSchemaUpdate(SchemaFor(f(full.schemaFor.schema), full.schemaFor.fieldMapper))
    case _                      => update
  }
}
