package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.NoUpdate

import scala.reflect.runtime.universe._

class DefinitionEnvironment[Typeclass[_]](
    map: Map[WeakTypeTag[_], Typeclass[_]] = Map.empty[WeakTypeTag[_], Typeclass[_]]) {

  def updated[T: WeakTypeTag, R](typeclass: Typeclass[T]): DefinitionEnvironment[Typeclass] =
    new DefinitionEnvironment[Typeclass](map.updated(implicitly[WeakTypeTag[T]], typeclass))

  def get[T: WeakTypeTag]: Option[Typeclass[T]] = map.get(implicitly[WeakTypeTag[T]]).asInstanceOf[Option[Typeclass[T]]]
}

object DefinitionEnvironment {
  def empty[Typeclass[_]] = new DefinitionEnvironment[Typeclass]()
}

trait Resolvable[Typeclass[_], T] {

  def apply(env: DefinitionEnvironment[Typeclass], update: SchemaUpdate): Typeclass[T]

  def apply(): Typeclass[T] = apply(DefinitionEnvironment.empty, NoUpdate)
}
