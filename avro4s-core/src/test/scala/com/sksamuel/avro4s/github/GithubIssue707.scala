package com.sksamuel.avro4s.github

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.scalatest.funsuite.AnyFunSuite

case class Holder(parent:Parent)
sealed trait Parent
case class ChildA() extends Parent


class GithubIssue707 extends AnyFunSuite {

  test("Serializable Encoder[Holder] #432") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Encoder[Holder]])
    oos.close()
  }

  test("Serializable Decoder[Holder] #432") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Decoder[Holder]])
    oos.close()
  }

  test("Serializable SchemaFor[Holder] works") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[SchemaFor[Holder]])
    oos.close()
  }

  test("Serializable Encoder[Holder] works after calling withSchema") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Encoder[Holder]].withSchema(implicitly[SchemaFor[Holder]]))
    oos.close()
  }

  test("Serializable Decoder[Holder] works after calling withSchema") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Decoder[Holder]].withSchema(implicitly[SchemaFor[Holder]]))
    oos.close()
  }


  test("Serializable Encoder[Parent]") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Encoder[Parent]])
    oos.close()
  }

  test("Serializable Decoder[Parent]") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Decoder[Parent]])
    oos.close()
  }

  test("Serializable SchemaFor[Parent]") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[SchemaFor[Parent]])
    oos.close()
  }

  test("Serializable Encoder[Parent] works after calling withSchema") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Encoder[Parent]].withSchema(implicitly[SchemaFor[Parent]]))
    oos.close()
  }

  test("Serializable Decoder[Parent] works after calling withSchema") {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(implicitly[Decoder[Parent]].withSchema(implicitly[SchemaFor[Parent]]))
    oos.close()
  }
}
