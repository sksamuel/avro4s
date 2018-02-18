package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil, Coproduct, Inl, Inr}

case class Department(name: String, head: Employee)
sealed trait Employee {
  def name: String
}
final case class RankAndFile(name: String, jobTitle: String) extends Employee
final case class BigBoss(name: String) extends Employee

class RecordFormatTest extends WordSpec with Matchers {
  case class Composer(name: String, birthplace: String, compositions: Seq[String])

  case class Book(title: String)
  case class Song(title: String, lyrics: String)
  case class Author[T](name: String, birthplace: String, work: Seq[T])

  val joe = RankAndFile("Joe", "grunt")
  val bob = BigBoss("Bob")

  val sales = Department("sales", bob)
  val floor = Department("floor", joe)

  "RecordFormat" should {
    "convert to/from record" in {
      val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
      val record = RecordFormat[Composer].to(ennio)
      record.toString shouldBe """{"name": "ennio morricone", "birthplace": "rome", "compositions": ["legend of 1900", "ecstasy of gold"]}"""
      val after = RecordFormat[Composer].from(record)
      after shouldBe ennio
    }

    "convert to/from record for type contained in uppercase package" in {
      val data = examples.UppercasePkg.Data(Inr(Inl(5)))
      val fmt = RecordFormat[examples.UppercasePkg.Data]
      fmt.from(fmt.to(data)) shouldBe data
    }

    "convert to/from records containing sealed trait hierarchy" in {
      val wrapper1 = Wrapper(Wobble("abc"))
      val wrapper2 = Wrapper(Wabble(3.14))
      val fmt = RecordFormat[Wrapper]
      fmt.from(fmt.to(wrapper1)) shouldBe wrapper1
      fmt.from(fmt.to(wrapper2)) shouldBe wrapper2
    }

    "support sealed traits with members" in {
      val fmt = RecordFormat[Department]
      fmt.from(fmt.to(sales)) shouldBe sales
      fmt.from(fmt.to(floor)) shouldBe floor
    }

    "convert to/from records of generic classes" in {
      val author1 = Author[Book]("Heraclitus", "Ephesus", Seq(Book("Panta Rhei")))
      val fmt = RecordFormat[Author[Book]]
      fmt.from(fmt.to(author1)) shouldBe author1
    }

    "strings handled if not instances of 'Utf8' class" in {
      type Value = Int :+: String :+: Boolean :+: CNil
      val format = RecordFormat[Union]

      val first = Union(Coproduct[Value]("ryan"))
      val second = Union(Coproduct[Value](35))

      val record1 = format.to(first)
      val record2 = format.to(second)

      val result1 = format.from(record1)
      val result2 = format.from(record2)

      result1 should be(first)
      result2 should be(second)
    }
  }
}
