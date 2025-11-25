package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class Github411  extends AnyFunSuite with Matchers {

  case class DefaultOfDefault(value: String)
  case class DefaultValue(property: DefaultOfDefault = DefaultOfDefault("some-default"))
  case class Github411Class(property: DefaultValue = DefaultValue())

  test("schema generation with defaults in defaults") {
    SchemaFor[Github411Class].schema.toString(true)
  }
}