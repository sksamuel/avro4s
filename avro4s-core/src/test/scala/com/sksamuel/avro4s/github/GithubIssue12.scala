package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class GithubIssue12 extends AnyWordSpec with Matchers {

  case class SalesRecord(val saleId: Int,
                         val sellerId: Int,
                         private val createDateStr: String,
                         val vin: String,
                         val make: String,
                         val model: String,
                         val year: Double,
                         val tL: String,
                         val clr: String,
                         val tr: String,
                         val eg: String,
                         val iT: String,
                         val tML: Option[String],
                         val sCPN: Option[String],
                         val sEA: Option[String],
                         val sET: Option[String],
                         val sCN: Option[String])

  "avro schema" should {
    "work for private vals" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github12.json"))
      val schema = AvroSchema[SalesRecord]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
