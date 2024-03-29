package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{Avro4sException, AvroName, AvroNamespace, AvroSchema, Decoder, Encoder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import com.sksamuel.avro4s.SchemaFor

class Scala3EnumEncoderTest extends AnyFunSuite with Matchers {

  test("support non-parametrized, non-extending Scala 3 enums") {
    val schema = AvroSchema[Simple]
    val record = Encoder[Simple].encode(schema).apply(Simple.ONE).asInstanceOf[GenericData.EnumSymbol]
    schema.getType shouldBe org.apache.avro.Schema.Type.ENUM
    record.toString shouldBe "ONE"
  }

  test("support non-parametrized, extending Scala 3 enums") {
    val schema = AvroSchema[Extending]
    val record = Encoder[Extending].encode(schema).apply(Extending.TWO).asInstanceOf[GenericData.EnumSymbol]
    schema.getType shouldBe org.apache.avro.Schema.Type.ENUM
    record.toString shouldBe "TWO"
  }

  test("support parametrized Scala 3 enums") {
    val schema = AvroSchema[ParametrizedWithInt]
    val record = Encoder[ParametrizedWithInt].encode(schema).apply(ParametrizedWithInt.THREE).asInstanceOf[GenericData.EnumSymbol]
    schema.getType shouldBe org.apache.avro.Schema.Type.ENUM
    record.toString shouldBe "THREE"
  }

  test("support non-parametrized, non-extending Scala 3 annotated enums") {
    val schema = AvroSchema[SimpleAnnotated]
    val record = Encoder[SimpleAnnotated].encode(schema).apply(SimpleAnnotated.ONE).asInstanceOf[GenericData.EnumSymbol]
    schema.getType shouldBe org.apache.avro.Schema.Type.ENUM
    schema.getNamespace shouldBe "com.test.namespace"
    schema.getName shouldBe "CustomSimple"
    schema.getEnumSymbols should contain("ONE")
  }

  test("support non-parametrized, extending Scala 3 annotated enums") {
    val schema = AvroSchema[ExtendingAnnotated]
    val record = Encoder[ExtendingAnnotated].encode(schema).apply(ExtendingAnnotated.TWO).asInstanceOf[GenericData.EnumSymbol]
    schema.getType shouldBe org.apache.avro.Schema.Type.ENUM
    schema.getNamespace shouldBe "com.test.namespace"
    schema.getName shouldBe "CustomExtended"
    schema.getEnumSymbols should contain("TWO")
  }

  test("support parametrized Scala 3 annotated enums") {
    val schema = AvroSchema[ParametrizedWithIntAnnotated]
    val record = Encoder[ParametrizedWithIntAnnotated].encode(schema).apply(ParametrizedWithIntAnnotated.THREE).asInstanceOf[GenericData.EnumSymbol]
    schema.getType shouldBe org.apache.avro.Schema.Type.ENUM
    schema.getNamespace shouldBe "com.test.namespace"
    schema.getName shouldBe "CustomParametrizedWithInt"
    record.toString shouldBe "THREE"
  }

  test("support Scala 3 enums that are parametrized with a product") {
    val schema = AvroSchema[WithProduct]
    val record = Encoder[WithProduct].encode(schema).apply(WithProduct.One).asInstanceOf[GenericData.EnumSymbol]
    schema.getType shouldBe org.apache.avro.Schema.Type.ENUM
    record.toString shouldBe "One"
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

@AvroNamespace("com.test.namespace")
@AvroName("CustomSimple")
enum SimpleAnnotated {
  case ONE, TWO, THREE
}

@AvroNamespace("com.test.namespace")
@AvroName("CustomExtended")
enum ExtendingAnnotated {
  case ONE extends ExtendingAnnotated
  case TWO extends ExtendingAnnotated
  case THREE extends ExtendingAnnotated
}

@AvroNamespace("com.test.namespace")
@AvroName("CustomParametrizedWithInt")
enum ParametrizedWithIntAnnotated(val param: Int) {
  case ONE extends ParametrizedWithIntAnnotated(1)
  case TWO extends ParametrizedWithIntAnnotated(2)
  case THREE extends ParametrizedWithIntAnnotated(3)
}


final case class TestProduct(a: String, b: Int)

enum WithProduct(product: TestProduct) {
  case One extends WithProduct(TestProduct("a", 1))
  case Two extends WithProduct(TestProduct("b", 2))
}
