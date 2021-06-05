//package com.sksamuel.avro4s.github
//
//import com.sksamuel.avro4s.{Decoder, TypeGuardedDecoding}
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//
//import scala.reflect.ClassTag
//import scala.reflect.runtime.universe._
//
//import scala.collection.JavaConverters._
//
//class GithubIssue545 extends AnyWordSpec with Matchers {
//  "TypeGuardedDecoding" should {
//    "create a map decoder instead of an array decoder" in {
//      val mapDecoder = Decoder.mapDecoder[String]
//
//      val typeGuard = TypeGuardedDecoding[Map[String, String]].guard(mapDecoder)
//      val value = Map().asJava
//
//      typeGuard.isDefinedAt(value) shouldBe true
//    }
//  }
//}
