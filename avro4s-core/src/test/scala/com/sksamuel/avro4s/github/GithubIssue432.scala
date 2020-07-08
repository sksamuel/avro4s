package com.sksamuel.avro4s.github

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.sksamuel.avro4s.Encoder
import org.scalatest.funsuite.AnyFunSuite

class GithubIssue432 extends AnyFunSuite {

  test("Serializable Encoder[BigDecimal] #432") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(Encoder.bigDecimalEncoder)
    oos.close()
  }

  test("Deserialized Encoder[BigDecimal] works") {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(Encoder.bigDecimalEncoder)
    oos.close()

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val encoder = ois.readObject().asInstanceOf[Encoder[BigDecimal]]

    encoder.encode(12.34)
  }
}
