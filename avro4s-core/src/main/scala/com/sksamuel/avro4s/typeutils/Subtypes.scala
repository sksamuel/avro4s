package com.sksamuel.avro4s.typeutils

import magnolia1.SealedTrait

object SubtypeOrdering extends Ordering[SealedTrait.Subtype[_, _, _]] {
  override def compare(a: SealedTrait.Subtype[_, _, _], b: SealedTrait.Subtype[_, _, _]): Int = {

    val annosA = new Annotations(a.annotations)
    val annosB = new Annotations(b.annotations)

    val namesA = new Names(a.typeInfo, annosA)
    val namesB = new Names(b.typeInfo, annosB)

    val priorityA = annosA.sortPriority.getOrElse(999999F)
    val priorityB = annosB.sortPriority.getOrElse(999999F)

    if (priorityA == priorityB) namesA.fullName.compare(namesB.fullName) else priorityB.compare(priorityA)
  }
}
