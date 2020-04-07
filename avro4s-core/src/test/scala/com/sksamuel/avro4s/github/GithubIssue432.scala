package com.sksamuel.avro4s.github

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import com.sksamuel.avro4s.Encoder
import org.scalatest.{FunSuite, Matchers}

class GithubIssue432 extends FunSuite with Matchers {

  test("Serializable Encoder[BigDecimal] #432") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(Encoder.bigDecimalEncoder)
    oos.close
  }
}