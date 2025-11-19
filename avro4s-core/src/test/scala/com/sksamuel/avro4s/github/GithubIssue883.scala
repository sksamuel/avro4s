package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GithubIssue883 extends AnyWordSpec with Matchers {
  "OptionSchemas" should {
    "not mutate original schema" in {
      sealed trait Inner
      case class A(v: String) extends Inner
      case class B(v: String) extends Inner
      case class Outer(v: Option[Inner])
      implicit val schemaForInner: SchemaFor[Inner] = SchemaFor.derived[Inner]
      implicit val schemaForOuter: SchemaFor[Outer] = SchemaFor.derived[Outer]
      schemaForInner.schema.getTypes.size shouldBe 2 // union A,B
      schemaForOuter.schema.getField("v").schema.getTypes.size shouldBe 3 // union null,A,B
    }
  }
}
