package com.sksamuel.avro4s.streams.output

import com.sksamuel.avro4s.internal.AvroSchema
import com.sksamuel.avro4s.schema.{Colours, Wine}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

class EnumDataOutputStreamTest extends FunSuite with Matchers with OutputStreamTest {

  test("java enum") {
    case class Test(z: Wine)
    val schema = AvroSchema[Wine]
    writeRead(Test(Wine.Malbec)) { record =>
      record.get("z") shouldBe new GenericData.EnumSymbol(schema, Wine.Malbec)
    }
  }

  test("optional java enum") {
    case class Test(z: Option[Wine])
    val schema = AvroSchema[Wine]
    writeRead(Test(Some(Wine.Malbec))) { record =>
      record.get("z") shouldBe new GenericData.EnumSymbol(schema, Wine.Malbec)
    }
    writeRead(Test(None)) { record =>
      record.get("z") shouldBe null
    }
  }

  test("scala enum") {
    case class Test(z: Colours.Value)
    val schema = AvroSchema[Wine]
    writeRead(Test(Colours.Green)) { record =>
      record.get("z") shouldBe new GenericData.EnumSymbol(schema, Colours.Green)
    }
  }

  test("optional scala enum") {
    case class Test(z: Option[Colours.Value])
    val schema = AvroSchema[Wine]
    writeRead(Test(Some(Colours.Green))) { record =>
      record.get("z") shouldBe new GenericData.EnumSymbol(schema, Colours.Green)
    }
    writeRead(Test(None)) { record =>
      record.get("z") shouldBe null
    }
  }
}
