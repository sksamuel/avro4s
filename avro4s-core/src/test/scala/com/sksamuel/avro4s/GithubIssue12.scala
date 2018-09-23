//package com.sksamuel.avro4s
//
//import org.scalatest.{WordSpec, Matchers}
//
//class GithubIssue12 extends WordSpec with Matchers {
//
//  case class SalesRecord(val saleId: Int,
//                         val sellerId: Int,
//                         private val createDateStr: String,
//                         val vin: String,
//                         val make: String,
//                         val model: String,
//                         val year: Double,
//                         val tL: String,
//                         val clr: String,
//                         val tr: String,
//                         val eg: String,
//                         val iT: String,
//                         val tML: Option[String],
//                         val dO: Double,
//                         val sP: Double,
//                         val tG: Double,
//                         val fEG: Double,
//                         val bEG: Double,
//                         val cT: Option[String],
//                         val sT: Option[String],
//                         private val cDStr: String,
//                         private val dDStr: String,
//                         private val rDStr: String,
//                         private val pBID: String,
//                         val fN: Option[String],
//                         val mN: Option[String],
//                         val lN: Option[String],
//                         val fuN: Option[String],
//                         val ad: Option[String],
//                         val ct: Option[String],
//                         val st: Option[String],
//                         private val pCStr: String,
//                         val hPN: Option[String],
//                         val bPN: Option[String],
//                         val cPN: Option[String],
//                         val eA: Option[String],
//                         val eT: Option[String],
//                         val cN: Option[String],
//                         val sBID: Option[String],
//                         val sFN: Option[String],
//                         val sMN: Option[String],
//                         val sLN: Option[String],
//                         val sFuMN: Option[String],
//                         val sAd: Option[String],
//                         val sC: Option[String],
//                         val sS: Option[String],
//                         val sPC: Option[String],
//                         val sHPN: Option[String],
//                         val sBPN: Option[String],
//                         val sCPN: Option[String],
//                         val sEA: Option[String],
//                         val sET: Option[String],
//                         val sCN: Option[String])
//
//  "avro schema" should {
//    "work for private vals" in {
//      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github12.avsc"))
//      val schema = AvroSchema[SalesRecord]
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//  }
//}
