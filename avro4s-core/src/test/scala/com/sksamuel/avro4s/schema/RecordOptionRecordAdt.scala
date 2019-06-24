package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FunSuite, Matchers}

// https://github.com/sksamuel/avro4s/issues/334
class RecordOptionRecordAdt extends FunSuite with Matchers {

  test("records with optional records with adt"){
    val schema = AvroSchema[HasOption]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/record_option_record_adt.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

sealed trait Adt
case object One extends Adt
case object Two extends Adt

case class HasAdt(adt: Adt)

case class HasOption(option: Option[HasAdt])
