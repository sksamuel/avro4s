package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.AvroName
import org.apache.avro.specific.FixedSize
import org.scalatest.{FunSuite, Matchers}

@FixedSize(12)
case class Artist(@AvroName("foo") name: String, birthplace: String, works: Seq[String])

class MacroTest extends FunSuite with Matchers {

  test("macro test") {
    val classrep = SchemaMacros.apply[Artist]
    println(classrep)
  }
}
