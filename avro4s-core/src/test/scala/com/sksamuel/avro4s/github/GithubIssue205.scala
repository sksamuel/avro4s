package com.sksamuel.avro4s.github

import org.scalatest.{Matchers, WordSpec}

class GithubIssue205 extends WordSpec with Matchers {

  "SchemaFor" should {
    "work for case classes with complete path and no default value" in {
      """
         import com.sksamuel.avro4s.internal.SchemaFor
         SchemaFor[com.sksamuel.avro4s.github.TestModel.Clazz1]
      """ should compile
    }

    "work for case classes with complete path and default value" in {
      """
         import com.sksamuel.avro4s.internal.SchemaFor
         SchemaFor[com.sksamuel.avro4s.github.TestModel.Clazz2]
      """ should compile
    }

    "work for case classes with import and no default value" in {
      """
         import com.sksamuel.avro4s.internal.SchemaFor
         import com.sksamuel.avro4s.github.TestModel._
         SchemaFor[Clazz1]
      """ should compile
    }

    "work for case classes with import and default value" in {
      """
         import com.sksamuel.avro4s.internal.SchemaFor
         import com.sksamuel.avro4s.github.TestModel._
         SchemaFor[Clazz2]
      """ should compile
    }
  }

}

object TestModel {
  case class Clazz1(str: String)
  case class Clazz2(str: String = "test")
}