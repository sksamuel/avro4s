package com.sksamuel.avro4s

import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr}

trait CoproductEncoders  {

//  implicit def genCoproductEncoder[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
//                                                      coproductEncoder: Encoder[C]): Encoder[T] = new Encoder[T] {
//    import scala.reflect.runtime.universe._
//    val tpe = weakTypeTag[T]
//    override def encode(value: T, schema: Schema): AnyRef = coproductEncoder.encode(gen.to(value), schema)
//  }
//
//  // A coproduct is a union, or a generalised either.
//  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.
//
//  // Shapeless's implementation builds up the type recursively,
//  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))
//
//  // `encode` here should never actually be invoked, because you can't
//  // actually construct a value of type a: CNil, but the Encoder[CNil]
//  // needs to exist to supply a base case for the recursion.
//  implicit def cnilEncoder: Encoder[CNil] = new Encoder[CNil] {
//    override def encode(t: CNil, schema: Schema): AnyRef = sys.error("This should never happen: CNil has no inhabitants")
//  }
//
//  // A :+: B is either Inl(value: A) or Inr(value: B), continuing the recursion
//  implicit def coproductEncoder[S, T <: Coproduct](implicit encoderS: Encoder[S], encoderT: Encoder[T]): Encoder[S :+: T] = new Encoder[S :+: T] {
//    override def encode(value: S :+: T, schema: Schema): AnyRef = {
//      value match {
//        case Inl(s) => encoderS.encode(s, schema)
//        case Inr(t) => encoderT.encode(t, schema)
//      }
//    }
//  }
}
