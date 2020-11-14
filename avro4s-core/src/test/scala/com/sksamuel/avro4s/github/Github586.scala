package com.sksamuel.avro4s.github

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object SampleV1 {
  sealed trait Fruit
  @AvroUnionDefault
  case object Unknown extends Fruit
  case class Mango(size: Int) extends Fruit
  case class Orange(size: Int) extends Fruit
  case class Lemon(size: Int) extends Fruit
  case class Banana(size: Int) extends Fruit
}

object SampleV2 {
  sealed trait Fruit
  @AvroUnionDefault
  case object Unknown extends Fruit
  case class Mango(size: Int, color: String) extends Fruit // new field without default value
  case class Orange(size: Int, color: String = "orange") extends Fruit // new field with default value
  case class Lemon(color: String) extends Fruit // new field, incompatible with prev one
  case class Banana(size: Int) extends Fruit
  case class Apple(size: Int) extends Fruit  // new type
}

class Github586 extends AnyFunSuite with Matchers {

  test("Schema union elements order") {
    val v1 = AvroSchema[SampleV1.Fruit]
    assert(v1.isUnion)
    assert(v1.getTypes.get(0).getName == "Unknown")
    assert(v1.getTypes.get(1).getName == "Banana")
    assert(v1.getTypes.get(2).getName == "Lemon")
    assert(v1.getTypes.get(3).getName == "Mango")
    assert(v1.getTypes.get(4).getName == "Orange")

    val v2 = AvroSchema[SampleV2.Fruit]
    assert(v2.isUnion)
    assert(v2.getTypes.get(0).getName == "Unknown")
    assert(v2.getTypes.get(1).getName == "Apple")
    assert(v2.getTypes.get(2).getName == "Banana")
    assert(v2.getTypes.get(3).getName == "Lemon")
    assert(v2.getTypes.get(4).getName == "Mango")
    assert(v2.getTypes.get(5).getName == "Orange")
  }

  test("Backward compatibility: Banana V2 => V1 expected V1.Banana because it is not changed") {
    val bytes = serializeV2(SampleV2.Banana(81))
    val result = deserializeV2toV1(bytes)

    assert(result == SampleV1.Banana(81))
  }

  test("Backward compatibility: Mango V2 => V1 expected V1.Mango because new field is ignored") {
    val bytes = serializeV2(SampleV2.Mango(81, "green"))
    val result = deserializeV2toV1(bytes)

    assert(result == SampleV1.Mango(81))
  }

  // Require AvroUnionDefault
  test("Backward compatibility: Apple V2 => V1 expected Unknown because Apple doesn't exists in V1") {
    val bytes = serializeV2(SampleV2.Apple(81))
    val result = deserializeV2toV1(bytes)

    assert(result == SampleV1.Unknown)
  }


  test("Forward compatibility: Banana V1 => V2 expected V2.Banana because it is not changed") {
    val bytes = serializeV1(SampleV1.Banana(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Banana(81))
  }

  // Require AvroUnionDefault
  test("Forward compatibility: Mango V1 => V2 expected Unknown because color field is missing") {
    val bytes = serializeV1(SampleV1.Mango(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Unknown)
  }

  test("Forward compatibility: Orange V1 => V2 expected V2.Orange with default color value") {
    val bytes = serializeV1(SampleV1.Orange(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Orange(81))
  }

  // Require AvroUnionDefault
  test("Forward compatibility: Lemon V1 => V2 expected Unknown because of incompatible fields (color/size)") {
    val bytes = serializeV1(SampleV1.Lemon(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Unknown)
  }

  private def serializeV2(value: SampleV2.Fruit): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[SampleV2.Fruit].to(stream).build()
    try {
      output.write(value)
      output.flush()
      stream.toByteArray
    } finally {
      output.close()
    }
  }

  private def deserializeV2toV1(value: Array[Byte]): SampleV1.Fruit = {
    val stream = AvroInputStream.binary[SampleV1.Fruit].from(value).build(AvroSchema[SampleV2.Fruit])
    try {
      stream.iterator.toSeq.head
    } finally {
      stream.close()
    }
  }

  private def serializeV1(value: SampleV1.Fruit): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[SampleV1.Fruit].to(stream).build()
    try {
      output.write(value)
      output.flush()
      stream.toByteArray
    } finally {
      output.close()
    }
  }

  private def deserializeV1toV2(value: Array[Byte]): SampleV2.Fruit = {
    val stream = AvroInputStream.binary[SampleV2.Fruit].from(value).build(AvroSchema[SampleV1.Fruit])
    try {
      stream.iterator.toSeq.head
    } finally {
      stream.close()
    }
  }
}