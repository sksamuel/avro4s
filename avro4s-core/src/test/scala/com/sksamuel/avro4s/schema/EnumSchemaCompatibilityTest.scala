//package com.sksamuel.avro4s.schema
//
//import com.sksamuel.avro4s.{AvroName, AvroSchema, ScalaEnumSchemaFor, SchemaFor}
//import org.apache.avro.{Schema, SchemaCompatibility}
//import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//
///**
// * The following tests uses the AVRO SchemaCompatibility.checkReaderWriterCompatibility method to check the
// * forwards and backwards compatibility of two versions of an enum schema.
// *
// * The first version of the enum has values are "Red", "Amber", and "Green". The second version adds "Orange".
// */
//class EnumSchemaCompatibilityTest extends AnyWordSpec with Matchers {
//
//  @AvroName("Colours")
//  object Colours1 extends Enumeration {
//    val Red, Amber, Green = Value
//  }
//
//  @AvroName("Colours")
//  object Colours2 extends Enumeration {
//    val Red, Amber, Green, Orange = Value
//  }
//
//  "An enum schema that does not contain a default enum value" should {
//
//    val schemaVersion1: Schema = AvroSchema[Colours1.Value]
//    val schemaVersion2: Schema = AvroSchema[Colours2.Value]
//
//    "not be backwards compatible when a new enum value is added" in {
//
//      val compatibilityType = SchemaCompatibility.checkReaderWriterCompatibility(
//        schemaVersion1,
//        schemaVersion2
//      ).getType
//
//      compatibilityType shouldEqual SchemaCompatibilityType.INCOMPATIBLE
//    }
//
//    "be forwards compatible even when a new enum value is added" in {
//
//      val compatibilityType = SchemaCompatibility.checkReaderWriterCompatibility(
//        schemaVersion2,
//        schemaVersion1
//      ).getType
//
//      compatibilityType shouldEqual SchemaCompatibilityType.COMPATIBLE
//    }
//  }
//
//  "an enum schema that contains a default enum value" should {
//
//    // define the enum schemas with a default value
//    implicit val schemaForColour1: SchemaFor[Colours1.Value] = ScalaEnumSchemaFor[Colours1.Value](Colours1.Amber)
//    implicit val schemaForColour2: SchemaFor[Colours2.Value] = ScalaEnumSchemaFor[Colours2.Value](Colours2.Amber)
//
//    val schemaVersion1: Schema = AvroSchema[Colours1.Value]
//    val schemaVersion2: Schema = AvroSchema[Colours2.Value]
//
//    "be backwards compatible when a new enum value is added" in {
//
//      val compatibilityType = SchemaCompatibility.checkReaderWriterCompatibility(
//        schemaVersion1,
//        schemaVersion2
//      ).getType
//
//      compatibilityType shouldEqual SchemaCompatibilityType.COMPATIBLE
//    }
//
//    "be forwards compatible when a new enum value is added" in {
//
//      val compatibilityType = SchemaCompatibility.checkReaderWriterCompatibility(
//        schemaVersion2,
//        schemaVersion1
//      ).getType
//
//      compatibilityType shouldEqual SchemaCompatibilityType.COMPATIBLE
//    }
//  }
//}
//
