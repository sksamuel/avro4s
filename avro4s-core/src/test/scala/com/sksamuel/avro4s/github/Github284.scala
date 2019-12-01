package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{Record, RecordFormat}
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.{AvroRuntimeException, Schema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Street(var name: String) extends SpecificRecordBase {
  def this() = this("")

  override def get(i: Int): AnyRef = i match {
    case 0 => name
    case _ => throw new AvroRuntimeException("Bad index")
  }

  override def put(index: Int, value: scala.Any): Unit =
    index match {
      case 0 =>
        value.asInstanceOf[String]
      case _ =>
        throw new AvroRuntimeException("Bad index")
    }

  override def getSchema: Schema = Street.SCHEMA$
}

object Street {
  val SCHEMA$ =
    (new Schema.Parser).parse("""
                                |{
                                | "type": "record",
                                | "namespace": "com.sksamuel.avro4s.github",
                                | "name": "Street",
                                | "fields": [
                                |     {"name": "name", "type": "string"}
                                | ]
                                |}
                              """.stripMargin)
}

final class Github284 extends AnyWordSpec with Matchers {
  "SchemaFor" should {
    "convert case class to a Record and convert it back to original case class" in {

      val street: Street = Street(name = "street name")

      val streetAsRecord: Record = RecordFormat[Street].to(street)

      val decodedStreet: Street = RecordFormat[Street].from(streetAsRecord)

      streetAsRecord shouldBe a [Record]

      decodedStreet shouldBe a [Street]

      decodedStreet shouldBe street
    }
  }
}
