//package com.sksamuel.avro4s
//
//import org.scalatest.{WordSpec, Matchers}
//
//class GithubIssue187And152 extends WordSpec with Matchers {
//  import GithubIssue187And152._
//
//  "SchemaFor" should {
//    "work for a simple ADT" in {
//      SchemaFor[Product1]
//    }
//  }
//  "Encoding and decoding a nested ADT" should {
//    "work" in {
//      val data = Product1(5, Product1.Coproduct1.Coproduct1Summand2(7, Product1.Coproduct1.Coproduct1Summand2.Coproduct2.One))
//      import java.io.File
//      val file: File = new File("product1.avro")
//      val os = AvroOutputStream.data[Product1](file)
//      os.write(data)
//      os.close()
//      val is = AvroInputStream.data[Product1](file)
//      val decoded = is.iterator.toList
//
//      decoded shouldEqual List(data)
//    }
//  }
//}
//
//object GithubIssue187And152 {
//  final case class Product1(i: Int, j: Product1.Coproduct1)
//
//  object Product1 {
//    sealed trait Coproduct1
//
//    object Coproduct1 {
//      final case class Coproduct1Summand2(i: Int, j: Coproduct1Summand2.Coproduct2) extends Coproduct1
//
//      object Coproduct1Summand2 {
//        sealed trait Coproduct2
//
//        object Coproduct2 {
//          final case object One extends Coproduct2
//          final case object Two extends Coproduct2
//          final case class Three(s: String) extends Coproduct2
//          final case class Four() extends Coproduct2
//        }
//      }
//      case object Coproduct1Summand1 extends Coproduct1
//      case class Coproduct1Summand3() extends Coproduct1
//    }
//  }
//}