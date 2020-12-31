package com.sksamuel.avro4s.schemas

import org.apache.avro.Schema

import scala.collection.mutable.WeakHashMap
import scala.quoted.Type

/**
 * Helps with encoders/decoders/schemas for recursive types. Provides a map of already defined bindings for types.
 * For an Encoder e.g., captures which encoders are already defined when building the (cyclic) encoder graph for
 * a recursive type.
 *
 * Initially, one would expect that handling recursive types can be delegated to Magnolia. Magnolia uses definitions
 * and lazy evaluation to defer the evaluation of recursive types, so that the recursive definition of the type class
 * gets unrolled as needed during data processing. There are two reason why this approach doesn't work in avro4s: one
 * reason for SchemaFor, and one reason for both encoders adn decoders.
 *
 * a) For SchemaFor, the avro library requires to build a cyclic graph of records for recursive types. Lazily unfolding
 * doesn't work here. To give an example, consider the type
 *
 * {{{
 *      sealed trait Tree
 *      case class Leaf(value: Int) extends Tree
 *      case class Branch(left: Tree, right: Tree) extends Tree
 * }}}
 *
 * The Avro schema for this needs to be constructed by first creating the records, then the union, and then
 * setting the fields of the branch record with the schema of the tree union:
 *
 * {{{
 *     val leaf   = Schema.createRecord(...)
 *     val branch = Schema.createRecord(...)
 *     val tree   = Schema.createUnion(branch, leaf)
 *     branch.setFields(...) // using the tree schema just created before
 * }}}
 *
 * b) For encoder / decoder, leveraging Magnolias mechanism would work. However, the construction of encoders and decoders
 * is expensive as computing name mappings involves type reflection that would be re-computed on every unfold.
 * Additionally, annotations on one type may trickle down to other types, e.g., a namespace annotation on a trait
 * will be applied to all case classes below it. This push-down of modifications will have to be done on every
 * unfolding as well, meaning additional object allocations.
 *
 * So instead of using the Magnolia mechanism for encoder / decoder, we reuse our own recursive type handling
 * machinery that we need for SchemaFor, and by this 1) stay consistent within this code base, and 2) achieve much
 * better runtime performance on encoding and decoding (i.e. increase the throughput by roughly 10x).
 *
 * @tparam Typeclass Encoder, Decoder or SchemaFor
 */
class DefinitionEnvironment(set: Set[String]) {

//  /**
//   * Extend the environment with a definition for type `T` - uses a `WeakTypeTag` as entry key.
//   */
//  def update[T](fqn: String, typeclass: Any): DefinitionEnvironment = {
//    println(s"Setting $fqn = $typeclass")
//    new DefinitionEnvironment(map.addOne(fqn, typeclass))
//  }
//
//  /**
//   * Retrieve an already existing definition definition, given a `WeakTypeTag` as key.
//   */
//  // todo would prefer to use Type[T] instead of fqn if I can figure it out
//  def schemaFor[T](fqn: String): Option[SchemaFor[T]] = map.get(fqn).asInstanceOf[Option[SchemaFor[T]]]
  
  def contains(fqn: String) = set.contains(fqn) 
  def add(fqn: String) = new DefinitionEnvironment(set + fqn)
}

object DefinitionEnvironment {
  def empty = new DefinitionEnvironment(Set.empty)
}
