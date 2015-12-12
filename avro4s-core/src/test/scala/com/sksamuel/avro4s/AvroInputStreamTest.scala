package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroInputStreamTest extends WordSpec with Matchers with Timeouts {

  def write[T](ts: Seq[T])(implicit schema: AvroSchema2[T], ser: AvroSerializer[T]): Array[Byte] = {
    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream[T](output)
    avro.write(ts)
    avro.close()
    output.toByteArray
  }

  "AvroDeserializer" should {
    "read string" in {

      case class Test(str: String)

      val data = Seq(Test("sammy"), Test("hammy"))
      val bytes = write(data)

      val in = AvroInputStream[Test](bytes)
      in.iterator.toList shouldBe data.toList
      in.close()
    }
  }
}