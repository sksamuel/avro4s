//package com.sksamuel.avro4s
//
//import com.sksamuel.avro4s.schema.Union
//import org.scalatest.{Matchers, WordSpec}
//import shapeless.{:+:, CNil, Coproduct, Inl, Inr}
//
//case class Department(name: String, head: Employee)
//sealed trait Employee {
//  def name: String
//}
//final case class RankAndFile(name: String, jobTitle: String) extends Employee
//final case class BigBoss(name: String) extends Employee
//
//class RecordFormatTest extends WordSpec with Matchers {
//  case class Composer(name: String, birthplace: String, compositions: Seq[String])
//
//  case class Book(title: String)
//  case class Song(title: String, lyrics: String)
//  case class Author[T](name: String, birthplace: String, work: Seq[T])
//
//  val joe = RankAndFile("Joe", "grunt")
//  val bob = BigBoss("Bob")
//
//  val sales = Department("sales", bob)
//  val floor = Department("floor", joe)
//
//  "RecordFormat" should {
//
//    "convert to/from record for type contained in uppercase package" in {
//      val data = examples.UppercasePkg.Data(Inr(Inl(5)))
//      val fmt = RecordFormat[examples.UppercasePkg.Data]
//      fmt.from(fmt.to(data)) shouldBe data
//    }
//
//    "convert to/from records of generic classes" in {
//      val author1 = Author[Book]("Heraclitus", "Ephesus", Seq(Book("Panta Rhei")))
//      val fmt = RecordFormat[Author[Book]]
//      fmt.from(fmt.to(author1)) shouldBe author1
//    }
//  }
//}
