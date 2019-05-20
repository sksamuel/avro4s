//package com.sksamuel.avro4s.github
//
//import java.io.File
//
//import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, Encoder}
//import org.scalatest.{FunSuite, Matchers}
//
//class GithubIssue187And152 extends FunSuite with Matchers {
//
//  import GithubIssue187And152._
//
//  test("Encoding and decoding a nested ADT") {
//
//    val schema = AvroSchema[Product1]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github187.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//
//    val data = Product1(5, Product1.Coproduct1.Coproduct1Summand2(7, Product1.Coproduct1.Coproduct1Summand2.Coproduct2.One))
//
//    val encoder = Encoder[Product1]
//
//    val file: File = new File("product1.avro")
//    val os = AvroOutputStream.binary[Product1].to(file).build(schema)
//    os.write(data)
//    os.close()
//
//    val is = AvroInputStream.binary[Product1].from(file).build(schema)
//    val decoded = is.iterator.toList
//
//    decoded shouldEqual List(data)
//    file.delete()
//  }
//}
//
//object GithubIssue187And152 {
//
//  final case class Product1(i: Int, j: Product1.Coproduct1)
//
//  object Product1 {
//    sealed trait Coproduct1
//
//    object Coproduct1 {
//
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
//
//      case object Coproduct1Summand1 extends Coproduct1
//      case class Coproduct1Summand3() extends Coproduct1
//    }
//  }
//}