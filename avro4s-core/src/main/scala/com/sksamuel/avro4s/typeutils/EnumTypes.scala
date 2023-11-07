package com.sksamuel.avro4s.typeutils

import magnolia1.SealedTrait

object EnumOrdering extends Ordering[SealedTrait.Subtype[_, _, _]] {
  override def compare(a: SealedTrait.Subtype[_, _, _], b: SealedTrait.Subtype[_, _, _]): Int = {

    val annosA = new Annotations(a.annotations)
    val annosB = new Annotations(b.annotations)

    val priorityA = annosA.sortPriority.getOrElse(0F)
    val priorityB = annosB.sortPriority.getOrElse(0F)

    if (priorityA == priorityB) 0 else priorityA.compare(priorityB)
  }
}