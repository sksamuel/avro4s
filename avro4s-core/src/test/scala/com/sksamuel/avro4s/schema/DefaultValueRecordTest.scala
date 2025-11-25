package com.sksamuel.avro4s.schema


import com.sksamuel.avro4s.{AvroName, AvroSchema, FromRecord, ToRecord}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DefaultValueRecordTest extends AnyWordSpec with Matchers {

  "Converting to and from Avro GenericRecord" should {

    "use the default where appropriate" in {
      FromRecord[Jude2](AvroSchema[Jude2]).from(ToRecord[Jude](AvroSchema[Jude]).to(Jude())) shouldBe Jude2()
    }

  }

}

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