package com.sksamuel.avro4s

import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}

class ClassGeneratorTest extends WordSpec with Matchers {

  "com.sksamuel.avro4s.ClassGenerator" should {
    "generate class name" in {
      val defs = ClassGenerator(getClass.getResourceAsStream("/gameofthrones.avsc"))
      println(StringClassRenderer.render(defs))
      FileRenderer.render(Paths.get("."), defs)
    }
  }
}
