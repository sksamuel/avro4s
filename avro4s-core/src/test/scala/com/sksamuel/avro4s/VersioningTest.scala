//package com.sksamuel.avro4s
//
//import java.io.File
//
//import org.scalatest.concurrent.TimeLimits
//import org.scalatest.{Matchers, WordSpec}
//
//class VersioningTest extends WordSpec with Matchers with TimeLimits {
//
//  case class Person(name: String, age: Int)
//  case class PersonV2(name: String, age: Int, title: Option[String])
//  case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])
//
//  val peopleV2 = Seq(
//    PersonV2("p1", 10, Some("Mr")),
//    PersonV2("p2", 20, None)
//  )
//
//  "write version2 and read version1 and read version3" in {
//    val file = new File("target/peoplev2.avro")
//    val os = AvroOutputStream.data[PersonV2](file)
//    os.write(peopleV2)
//    os.close()
//
//    val peopleV1is = AvroInputStream.data[Person](file)
//    peopleV1is.iterator.toList shouldBe Seq(Person("p1", 10), Person("p2", 20))
//    peopleV1is.close()
//
//    val peopleV3is = AvroInputStream.data[PersonV3](file)
//    peopleV3is.iterator.toList shouldBe Seq(PersonV3("p1", 10, Some("Mr"), None), PersonV3("p2", 20, None, None))
//    peopleV3is.close()
//  }
//
//}
