package com.sksamuel.avro4s

import java.io.File

import org.apache.avro.Schema
import org.scalatest.concurrent.TimeLimits
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class AvroInputStreamBuilderTest extends WordSpec with Matchers with TimeLimits with BeforeAndAfter {

  case class Person(name: String, age: Int)
  case class PersonV2(name: String, age: Int, title: Option[String])
  case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])

  val people = Seq(
    Person("p1", 10),
    Person("p2", 20)
  )
  val peopleV2 = Seq(
    PersonV2("p1", 10, Some("Mr")),
    PersonV2("p2", 20, None)
  )
  val peopleV3 = Seq(
    PersonV3("p1", 10, Some("Mr"), None),
    PersonV3("p2", 20, None, None)
  )

  val personSchema: Schema = implicitly[SchemaFor[Person]].apply()
  val personV2Schema: Schema = implicitly[SchemaFor[PersonV2]].apply()

  val peopleV1FileData = new File("target/people_data.avro")
  val peopleV1FileBinary = new File("target/people_binary.avro")
  val peopleV1FileJson = new File("target/people_json.avro")
  val peopleV2FileData = new File("target/people_v2_data.avro")
  val peopleV2FileBinary = new File("target/people_v2_binary.avro")
  val peopleV2FileJson = new File("target/people_v2_json.avro")

  private def write[T](data: Seq[T], os: AvroOutputStream[T]) = {
    os.write(data)
    os.close()
  }

  before {
    write(people, AvroOutputStream.data[Person](peopleV1FileData))
    write(people, AvroOutputStream.binary[Person](peopleV1FileBinary))
    write(people, AvroOutputStream.json[Person](peopleV1FileJson))
    write(peopleV2, AvroOutputStream.data[PersonV2](peopleV2FileData))
    write(peopleV2, AvroOutputStream.binary[PersonV2](peopleV2FileBinary))
    write(peopleV2, AvroOutputStream.json[PersonV2](peopleV2FileJson))
  }

  private def testReadPeopleData(is: AvroInputStream[Person]): Unit = {
    is.iterator.toList shouldBe people
    is.close()
  }

  private def testReadPeopleV3Data(is: AvroInputStream[PersonV3]): Unit = {
    is.iterator.toList shouldBe peopleV3
    is.close()
  }

  "data builder without schema" in {
    testReadPeopleData(
      AvroInputStream
      .builder[Person](AvroInputStream.DataFormat)
      .from(peopleV1FileData)
      .build()
    )
  }

  "binary builder without schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.BinaryFormat)
        .from(peopleV1FileBinary)
        .build()
    )
  }

  "json builder without schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.JsonFormat)
        .from(peopleV1FileJson)
        .build()
    )
  }

  "data builder with default schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.DataFormat)
        .from(peopleV1FileData)
        .schema()
        .build()
    )
  }

  "binary builder with default schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.BinaryFormat)
        .from(peopleV1FileBinary)
        .schema()
        .build()
    )
  }

  "json builder with default schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.JsonFormat)
        .from(peopleV1FileJson)
        .schema()
        .build()
    )
  }

  "data builder with explicit schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.DataFormat)
        .from(peopleV1FileData)
        .schema(personSchema)
        .build()
    )
  }

  "binary builder with explicit schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.BinaryFormat)
        .from(peopleV1FileBinary)
        .schema(personSchema)
        .build()
    )
  }

  "json builder with explicit schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.JsonFormat)
        .from(peopleV1FileJson)
        .schema(personSchema)
        .build()
    )
  }

  "data builder with explicit writer, reader schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.DataFormat)
        .from(peopleV2FileData)
        .schema(personV2Schema, personSchema)
        .build()
    )
  }

  "binary builder with explicit writer, reader schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.BinaryFormat)
        .from(peopleV2FileBinary)
        .schema(personV2Schema, personSchema)
        .build()
    )
  }

  "json builder with explicit writer, reader schema" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.JsonFormat)
        .from(peopleV2FileJson)
        .schema(personV2Schema, personSchema)
        .build()
    )
  }

  "data builder with explicit writer, reader schemaWriterReader" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.DataFormat)
        .from(peopleV2FileData)
        .schemaWriterReader[PersonV2, Person]()
        .build()
    )
  }

  "binary builder with explicit writer, reader schemaWriterReader" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.BinaryFormat)
        .from(peopleV2FileBinary)
        .schemaWriterReader[PersonV2, Person]()
        .build()
    )
  }

  "json builder with explicit writer, reader schemaWriterReader" in {
    testReadPeopleData(
      AvroInputStream
        .builder[Person](AvroInputStream.JsonFormat)
        .from(peopleV2FileJson)
        .schemaWriterReader[PersonV2, Person]()
        .build()
    )
  }

  "data builder - read new version" in {
    // This test doesn't work for Binary and Json format, because of using GenericDatumReader.read() method
    // which requires not null reader schema
    testReadPeopleV3Data(
      AvroInputStream
        .builder[PersonV3](AvroInputStream.DataFormat)
        .from(peopleV2FileData)
        .build()
    )
  }

}
