//package com.sksamuel.avro4s
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.{GenericContainer, GenericData}
//import org.apache.avro.util.Utf8
//import shapeless.{:+:, CNil, Coproduct, Generic, Inr}
//
//trait CoproductDecoders  {
//
//  import scala.reflect.runtime.universe._
//
//  implicit def genCoproductDecoder[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
//                                                      decoder: Decoder[C]): Decoder[T] = new Decoder[T] {
//    override def decode(value: Any, schema: Schema): T = {
//      gen.from(decoder.decode(value, schema))
//    }
//  }
//
//  // A coproduct is a union, or a generalised either.
//  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.
//
//  // Shapeless's implementation builds up the type recursively,
//  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))
//
//  // `decode` here should never be invoked under normal operation; if
//  // we're trying to read a value of type CNil it's because we've
//  // tried all the other cases and failed. But the Decoder[CNil]
//  // needs to exist to supply a base case for the recursion.
//  implicit object CNilDecoderValue extends Decoder[CNil] {
//    override def decode(value: Any, schema: Schema): CNil = sys.error("This should never happen: CNil has no inhabitants")
//  }
//
//  // We're expecting to read a value of type S :+: T from avro.  Avro
//  // unions are untyped, so we have to attempt to read a value of type
//  // S (the concrete type), and if that fails, attempt to read the
//  // rest of the coproduct type T.
//
//  // thus, the bulk of the logic here is shared with reading Eithers, in `safeFrom`.
//  implicit def coproductDecoder[S: WeakTypeTag : Decoder, T <: Coproduct](implicit decoder: Decoder[T]): Decoder[S :+: T] = new Decoder[S :+: T] {
//    private[this] val safeFromS = makeSafeFrom[S]
//
//    override def decode(value: Any, schema: Schema): S :+: T = {
//      safeFromS.safeFrom(value, schema) match {
//        case Some(s) => Coproduct[S :+: T](s)
//        case None => Inr(decoder.decode(value, schema))
//      }
//    }
//  }
//
//  protected abstract class SafeFrom[T : Decoder] {
//    protected val decoder: Decoder[T] = implicitly[Decoder[T]]
//    def safeFrom(value: Any, schema: Schema): Option[T]
//  }
//

