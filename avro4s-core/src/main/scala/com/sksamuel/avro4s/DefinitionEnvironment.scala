package com.sksamuel.avro4s

import scala.reflect.runtime.universe._

/**
 * Helps with encoders/decoders/schemas for recursive types. Provides a map of already defined bindings for types.
 * For an Encoder e.g., captures which encoders are already defined when building the (cyclic) encoder graph for
 * a recursive type.
 *
 * @tparam Typeclass Encoder, Decoder or SchemaFor
 */
class DefinitionEnvironment[Typeclass[_]](
    map: Map[WeakTypeTag[_], Typeclass[_]] = Map.empty[WeakTypeTag[_], Typeclass[_]]) {

  /**
   * Extend the environment with a definition for type `T` - uses a `WeakTypeTag` as entry key.
   */
  def updated[T: WeakTypeTag](typeclass: Typeclass[T]): DefinitionEnvironment[Typeclass] =
    new DefinitionEnvironment[Typeclass](map.updated(implicitly[WeakTypeTag[T]], typeclass))

  /**
   * Retrieve an already existing definition definition, given a `WeakTypeTag` as key.
   */
  def get[T: WeakTypeTag]: Option[Typeclass[T]] = map.get(implicitly[WeakTypeTag[T]]).asInstanceOf[Option[Typeclass[T]]]
}

object DefinitionEnvironment {
  def empty[Typeclass[_]] = new DefinitionEnvironment[Typeclass]()
}