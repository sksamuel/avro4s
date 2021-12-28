package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{Avro4sException, AvroSchema, Decoder, Encoder}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class Scala3EnumDecoderTest extends AnyFunSuite with Matchers {

  test("support non-parametrized, non-extending Scala 3 enums") {
    val schema = AvroSchema[Simple]
    val record = GenericData.EnumSymbol(schema, "ONE")
    val decoded = Decoder[Simple].decode(schema)(record)    
    decoded shouldEqual Simple.ONE
  }

  test("supoort non-parametrized, extending Scala 3 enums") {
    val schema = AvroSchema[Extending]
    val record = GenericData.EnumSymbol(schema, "TWO")
    val decoded = Decoder[Extending].decode(schema)(record)    
    decoded shouldEqual Extending.TWO
  }

  test("supoort parametrized Scala 3 enums") {
    val schema = AvroSchema[ParametrizedWithInt]
    val record = GenericData.EnumSymbol(schema, "THREE")
    val decoded = Decoder[ParametrizedWithInt].decode(schema)(record)    
    decoded shouldEqual ParametrizedWithInt.THREE
  }
  
}

enum Simple {
  case ONE, TWO, THREE
}

enum Extending {
  case ONE extends Extending
  case TWO extends Extending
  case THREE extends Extending
}

enum ParametrizedWithInt(val param: Int) {
  case ONE extends ParametrizedWithInt(1)
  case TWO extends ParametrizedWithInt(2)
  case THREE extends ParametrizedWithInt(3)
}
