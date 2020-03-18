package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchemaV2, Encoder, FromRecord, ImmutableRecord, ToRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SchemaForUtf8Test extends AnyFunSpec with Matchers {

  describe("Serialization of objects containing Utf8 fields") {
    it("should serialize objects that contains simple Utf8 attributes") {
      case class Person(name: Utf8, alias: Utf8, age: Int)

      ToRecord[Person].to(Person(new Utf8("Name"), new Utf8("Alias"), 30))
    }

    it("should serialize objects that contains simple Utf8 attributes and one attribute has a default value") {
      case class Person(name: Utf8, alias: Utf8 = new Utf8("Not specified"), age: Int)

      ToRecord[Person].to(Person(name = new Utf8("Name"), age = 30))
    }

    it("should serialize objects that contains Optional Utf8 attributes") {
      case class Person(name: Utf8, alias: Option[Utf8], age: Int)

      ToRecord[Person].to(Person(new Utf8("Name"), Some(new Utf8("Alias")), 30))
      ToRecord[Person].to(Person(new Utf8("Name"), None, 30))
    }

    it("should serialize objects that contains Optional Utf8 attributes and one attribute has a default value") {
      case class Person(name: Utf8, alias: Option[Utf8] = Some(new Utf8("Not specified")), age: Int)

      ToRecord[Person].to(Person(new Utf8("Name"), Some(new Utf8("Alias")), 30))
      ToRecord[Person].to(Person(new Utf8("Name"), None, 30))
    }
  }

  describe("Deserialization of objects containing Utf8 fields") {
    it("should deserialize objects that contains simple Utf8 attributes") {
      case class Person(name: Utf8, alias: Utf8, age: Int)

      val record = ImmutableRecord(AvroSchemaV2[Person], Vector(new Utf8("Name"), new Utf8("Alias"), 30.asInstanceOf[AnyRef]))
      FromRecord[Person].from(record)
    }

    it("should deserialize objects that contains Optional Utf8 attributes") {
      case class Person(name: Utf8, familyName: Option[Utf8], alias: Option[Utf8], age: Int)

      val record = ImmutableRecord(AvroSchemaV2[Person], Vector(new Utf8("Name"), None, Some(new Utf8("Alias")), 30.asInstanceOf[AnyRef]))
      FromRecord[Person].from(record)
    }
  }

}
