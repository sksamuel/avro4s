package com.sksamuel.avro4s.schema


import com.sksamuel.avro4s.{AvroName, AvroSchema, FromRecord, ToRecord}
import org.scalatest.{Matchers, WordSpec}

class DefaultValueRecordTest extends WordSpec with Matchers {

  "Converting to and from Avro GenericRecord" should {

    "respect default case class values" in {

      FromRecord[Jude].from(ToRecord[Jude].to(Jude(Bobby("blah")))) shouldBe Jude(CKola)
    }

    "respect default case object values" in {
      FromRecord[Catcup].from(ToRecord[Catcup].to(Catcup())) shouldBe Catcup(Bobby("hates varg"))
    }

    "gtfo" in {
      println(ToRecord[Jude].to(Jude(CKola)))
      FromRecord[Jude2].from(ToRecord[Jude].to(Jude())) shouldBe Jude2()
    }

  }

}
sealed trait X
case object BX extends X


sealed trait Cup
case object CKola extends Cup
case class Bobby(hatesVarg: String) extends Cup

case class Catcup(cupcat: Cup = Bobby("hates varg"))
case class Jude(hatesVarg: Cup = CKola)

@AvroName("Cup")
sealed trait Cup2

object Cup2 {

  case class Bobby(hatesVarg: String) extends Cup2
}

case class Jude2(hatesVarg: Cup2 = Cup2.Bobby("hates varg"))