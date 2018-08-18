package com.sksamuel.avro4s.schema

import java.time.LocalDate

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.{Matchers, WordSpec}

class DateSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "support LocalDate as logical type date" in {
      case class LocalDateTest(localDate: LocalDate)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localdate.avsc"))
      val schema = SchemaFor[LocalDateTest]()
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

