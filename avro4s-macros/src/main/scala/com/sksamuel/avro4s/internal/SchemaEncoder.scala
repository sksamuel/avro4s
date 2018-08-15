package com.sksamuel.avro4s.internal

import java.io.Serializable

import org.apache.avro.Schema

trait SchemaEncoder[A] extends Serializable {
  self =>

  def encode(a: A): Schema

  /**
    * Create a new [[SchemaEncoder]] by applying a function to a value of type `B`
    * before encoding as an `A`.
    */
  final def contramap[B](f: B => A): SchemaEncoder[B] = new SchemaEncoder[B] {
    final def encode(b: B): Schema = self.encode(f(b))
  }

  /**
    * Create a new [[SchemaEncoder]] by applying a function to the output of this one.
    */
  final def map(f: Schema => Schema): SchemaEncoder[A] = new SchemaEncoder[A] {
    final def encode(a: A): Schema = f(self.encode(a))
  }
}
