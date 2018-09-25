package com.sksamuel.avro4s.github

import java.io.File

import com.sksamuel.avro4s.AvroOutputStream
import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{FunSuite, Matchers}

final case class SN(value: String) extends AnyVal
final case class SimpleUser(name: String, sn: Option[SN])

class GithubIssue191 extends FunSuite with Matchers {

  ignore("writing out AnyVal in an option") {
    implicit val schema = AvroSchema[SimpleUser]
    val out = AvroOutputStream.data[SimpleUser](new File("tmp.avro"), schema)
    out.write(SimpleUser("Tom", Some(SN("123"))))
  }
}
