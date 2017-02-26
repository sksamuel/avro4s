package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}
import shapeless.{Inl, Inr}

class RecordFormatTest extends WordSpec with Matchers {
  case class Composer(name: String, birthplace: String, compositions: Seq[String])
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
  }
}
