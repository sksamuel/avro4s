package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.github.SampleProtocol.SubPart1.InnerEnum
import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import org.scalatest.{FunSpec, Matchers}

object TopEnum extends Enumeration {
  type TopEnumVal = Value
  val v1, v2 = Value
}

case class WithTopEnum(topEnum: TopEnum.TopEnumVal)

object SampleProtocol {
  object SubPart1 {
    object InnerEnum extends Enumeration {
      type InnerEnum = Value
      val vv1, vv2 = Value
    }
  }
}

case class WithInnerEnum(ie: InnerEnum.InnerEnum)

class Githu4bIssue180 extends FunSpec with Matchers {

  describe("SchemaFor : FromRecord : ToRecord") {
    describe("with top level scala Enumerations") {
      val withTopEnum = WithTopEnum(TopEnum.v1)
      it("should be able to compile `FromRecord`") {
        FromRecord[WithTopEnum] shouldNot be(null)
      }
      it("should be able to compile `ToRecord`") {
        ToRecord[WithTopEnum] shouldNot be(null)
      }
      it("should be able to compile `SchemaFor`") {
        AvroSchema[WithTopEnum] shouldNot be(null)
      }
    }

    describe("with non-top level scala Enumerations") {
      val withInnerEnum = WithInnerEnum(InnerEnum.vv1)
      it("should be able to compile `FromRecord`") {
        FromRecord[WithInnerEnum] shouldNot be(null)
      }
      it("should be able to compile `ToRecord`") {
        ToRecord[WithInnerEnum] shouldNot be(null)
      }
      it("should be able to compile `SchemaFor`") {
        AvroSchema[WithInnerEnum] shouldNot be(null)
      }
    }
  }
}