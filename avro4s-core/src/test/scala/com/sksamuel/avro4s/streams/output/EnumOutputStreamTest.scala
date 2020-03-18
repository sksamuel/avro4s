package com.sksamuel.avro4s.streams.output

import com.sksamuel.avro4s.AvroSchemaV2
import com.sksamuel.avro4s.schema.{Colours, Wine}
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EnumOutputStreamTest extends AnyFunSuite with Matchers with OutputStreamTest {

  test("java enum") {
    case class Test(z: Wine)
    val schema = AvroSchemaV2[Wine]
    writeRead(Test(Wine.Malbec)) { record =>
      record.get("z") shouldBe GenericData.get.createEnum(Wine.Malbec.name, schema)
    }
  }

  test("optional java enum") {
    case class Test(z: Option[Wine])
    val schema = AvroSchemaV2[Wine]
    writeRead(Test(Some(Wine.Malbec))) { record =>
      record.get("z") shouldBe GenericData.get.createEnum(Wine.Malbec.name, schema)
    }
    writeRead(Test(None)) { record =>
      record.get("z") shouldBe null
    }
  }

  test("scala enum") {
    case class Test(z: Colours.Value)
    val schema = AvroSchemaV2[Wine]
    writeRead(Test(Colours.Green)) { record =>
      record.get("z") shouldBe GenericData.get.createEnum(Colours.Green.toString, schema)
    }
  }

  test("optional scala enum") {
    case class Test(z: Option[Colours.Value])
    val schema = AvroSchemaV2[Wine]
    writeRead(Test(Some(Colours.Green))) { record =>
      record.get("z") shouldBe GenericData.get.createEnum(Colours.Green.toString, schema)
    }
    writeRead(Test(None)) { record =>
      record.get("z") shouldBe null
    }
  }
}
