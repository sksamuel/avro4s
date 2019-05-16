package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FunSuite, Matchers}

import scala.language.higherKinds

sealed trait InnerTrait
case class InnerTraitConcrete(v: Int) extends InnerTrait
case class InnerTraitConcrete2(v: Int) extends InnerTrait

case class Inner(t: InnerTrait)
case class Outer(inners: Seq[Inner])

class GithubIssue281 extends FunSuite with Matchers {

  test("Avro Schema for a sealed trait in a subobject #281") {
    AvroSchema[Outer].toString(true) shouldBe """{
                                                |  "type" : "record",
                                                |  "name" : "Outer",
                                                |  "namespace" : "com.sksamuel.avro4s.github",
                                                |  "fields" : [ {
                                                |    "name" : "inners",
                                                |    "type" : {
                                                |      "type" : "array",
                                                |      "items" : {
                                                |        "type" : "record",
                                                |        "name" : "Inner",
                                                |        "fields" : [ {
                                                |          "name" : "t",
                                                |          "type" : [ {
                                                |            "type" : "record",
                                                |            "name" : "InnerTraitConcrete",
                                                |            "fields" : [ {
                                                |              "name" : "v",
                                                |              "type" : "int"
                                                |            } ]
                                                |          }, {
                                                |            "type" : "record",
                                                |            "name" : "InnerTraitConcrete2",
                                                |            "fields" : [ {
                                                |              "name" : "v",
                                                |              "type" : "int"
                                                |            } ]
                                                |          } ]
                                                |        } ]
                                                |      }
                                                |    }
                                                |  } ]
                                                |}""".stripMargin
  }
}
