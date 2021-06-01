//package com.sksamuel.avro4s
//
//import magnolia._
//
//import scala.language.experimental.macros
//import scala.reflect.runtime.universe._
//
//trait MagnoliaDerivedSchemaFors {
//  type Typeclass[T] = SchemaFor[T]
//
//  implicit def gen[T]: SchemaFor[T] = macro Magnolia.gen[T]
//
//  def dispatch[T: WeakTypeTag](ctx: SealedTrait[SchemaFor, T]): SchemaFor[T] = new ResolvableSchemaFor[T] {
//    def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[T] = {
//      env.get[T].getOrElse {
//        DatatypeShape.of[T] match {
//          case SealedTraitShape.TypeUnion => TypeUnions.schema(ctx, env, update)
//          case SealedTraitShape.ScalaEnum => SchemaFor[T](ScalaEnums.schema(ctx), DefaultFieldMapper)
//        }
//      }
//    }
//  }
//
//  def combine[T: WeakTypeTag](ctx: CaseClass[SchemaFor, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): SchemaFor[T] = {
//    val fm = fieldMapper
//    new ResolvableSchemaFor[T] {
//      def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[T] = {
//        env.get[T].getOrElse {
//          DatatypeShape.of(ctx) match {
//            case CaseClassShape.Record    => Records.schema(ctx, env, update, fm)
//            case CaseClassShape.ValueType => ValueTypes.schema(ctx, env, update)
//          }
//        }
//      }
//    }
//  }
//}
//
//trait MagnoliaDerivedEncoders {
//  type Typeclass[T] = Encoder[T]
//
//  implicit def gen[T]: Encoder[T] = macro Magnolia.gen[T]
//
//  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Encoder, T]): Encoder[T] = new ResolvableEncoder[T] {
//    def encoder(env: DefinitionEnvironment[Typeclass], update: SchemaUpdate): Typeclass[T] =
//      env.get[T].getOrElse {
//        DatatypeShape.of[T] match {
//          case SealedTraitShape.TypeUnion => TypeUnions.encoder(ctx, env, update)
//          case SealedTraitShape.ScalaEnum => ScalaEnums.encoder(ctx)
//        }
//      }
//  }
//
//  def combine[T: WeakTypeTag](ctx: CaseClass[Encoder, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): Encoder[T] = new ResolvableEncoder[T] {
//    def encoder(env: DefinitionEnvironment[Typeclass], update: SchemaUpdate): Typeclass[T] =
//      env.get[T].getOrElse {
//        DatatypeShape.of(ctx) match {
//          case CaseClassShape.Record    => Records.encoder(ctx, env, update, fieldMapper)
//          case CaseClassShape.ValueType => ValueTypes.encoder(ctx, env, update)
//        }
//      }
//  }
//}
//
//trait MagnoliaDerivedDecoders {
//  type Typeclass[T] = Decoder[T]
//
//  implicit def gen[T]: Decoder[T] = macro Magnolia.gen[T]
//
//  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Decoder, T]): Decoder[T] = new ResolvableDecoder[T] {
//    def decoder(env: DefinitionEnvironment[Typeclass], update: SchemaUpdate): Typeclass[T] =
//      env.get[T].getOrElse {
//        DatatypeShape.of[T] match {
//          case SealedTraitShape.TypeUnion => TypeUnions.decoder(ctx, env, update)
//          case SealedTraitShape.ScalaEnum => ScalaEnums.decoder(ctx)
//        }
//      }
//  }
//
//  def combine[T: WeakTypeTag](ctx: CaseClass[Decoder, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): Decoder[T] = new ResolvableDecoder[T] {
//    def decoder(env: DefinitionEnvironment[Typeclass], update: SchemaUpdate): Typeclass[T] =
//      env.get[T].getOrElse {
//        DatatypeShape.of(ctx) match {
//          case CaseClassShape.Record    => Records.decoder(ctx, env, update, fieldMapper)
//          case CaseClassShape.ValueType => ValueTypes.decoder(ctx, env, update)
//        }
//      }
//  }
//}
