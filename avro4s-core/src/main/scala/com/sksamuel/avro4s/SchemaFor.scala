package com.sksamuel.avro4s

import com.sksamuel.avro4s.avroutils.{EnumHelpers, SchemaHelper}
import org.apache.avro.SchemaBuilder

import scala.reflect.ClassTag
//package com.sksamuel.avro4s
//
//import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NoUpdate}
//import org.apache.avro.{Schema, SchemaBuilder}
//
//import scala.language.experimental.macros
//import scala.language.implicitConversions
//import scala.reflect.ClassTag
//import scala.reflect.runtime.universe._
//
///**
//  * A [[SchemaFor]] generates an Avro Schema for a Scala or Java type.
//  *
//  * For example, a String SchemaFor could return an instance of Schema.Type.STRING
//  * or Schema.Type.FIXED depending on the type required for Strings.
//
///**
//  * A SchemaFor that needs to be resolved before it is usable. Resolution is needed to properly setup SchemaFor instances
//  * for recursive types.
//  *
//  * If this instance is used without resolution, it falls back to use an adhoc-resolved instance and delegates all
//  * operations to it. This involves a performance penalty of lazy val access that can be avoided by
//  * calling [[SchemaFor.resolveSchemaFor]] and using that.
//  *
//  * For examples on how to define custom ResolvableSchemaFor instances, see the Readme and RecursiveSchemaTest.
//  *
//  * @tparam T type this schema is for (primitive type, case class, sealed trait, or enum e.g.).
//  */
//trait ResolvableSchemaFor[T] extends SchemaFor[T] {
//
//  /**
//    * Creates a SchemaFor instance (and applies schema changes given) or returns an already existing value from the
//    * given definition environment.
//    *
//    * @param env definition environment to use
//    * @param update schema update to apply
//    * @return either an already existing value from env or a new created instance.
//    */
//  def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[T]
//
//  lazy val adhocInstance = schemaFor(DefinitionEnvironment.empty, NoUpdate)
//
//  def schema = adhocInstance.schema
//  def fieldMapper: FieldMapper = adhocInstance.fieldMapper
//}
//


object JavaEnumSchemaFor {

  def apply[E <: Enum[_]](default: E)(using tag: ClassTag[E]): SchemaFor[E] =
    com.sksamuel.avro4s.schemas.Enums.schema[E].map[E](EnumHelpers.addDefault(default))
}

//object ScalaEnumSchemaFor extends EnumSchemaFor {
//
//  def apply[E <: scala.Enumeration#Value](default: E)(implicit tag: TypeTag[E]): SchemaFor[E] =
//    SchemaFor.scalaEnumSchemaFor.map[E](EnumHelpers.addDefault(default))
//}

//object SchemaFor
//    extends MagnoliaDerivedSchemaFors
//    with ShapelessCoproductSchemaFors
//    with CollectionAndContainerSchemaFors
//    with TupleSchemaFors
//    with ByteIterableSchemaFors
//    with BaseSchemaFors {
//
//  def apply[T](schema: Schema, fieldMapper: FieldMapper = DefaultFieldMapper) = {
//    val s = schema
//    val fm = fieldMapper
//    new SchemaFor[T] {
//      def schema: Schema = s
//      def fieldMapper: FieldMapper = fm
//    }
//  }
//
//  def apply[T](implicit schemaFor: SchemaFor[T]): SchemaFor[T] = schemaFor
//}
