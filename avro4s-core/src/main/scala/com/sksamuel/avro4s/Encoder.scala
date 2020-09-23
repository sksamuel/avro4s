package com.sksamuel.avro4s

import org.apache.avro.Schema

import scala.deriving.Mirror
import scala.deriving._
import scala.compiletime.{erasedValue, summonInline}

/**
 * An [[Encoder]] encodes a Scala value of type T into an [[AvroValue]]
 * based on the given schema.
 *
 * For example, encoding a string with a schema of type Schema.Type.STRING
 * would result in an instance of Utf8, whereas the same string and a
 * schema of type Schema.Type.FIXED would be encoded as an instance of GenericFixed.
 *
 * Another example is given a Scala enum value, and a schema of
 * type Schema.Type.ENUM, the value would be encoded as an instance
 * of GenericData.EnumSymbol.
 */
trait Encoder[T] {
  def encode(value: T, schema: Schema): AvroValue
}

trait BasicEncoders {
  
}

object Encoder {

  inline given derived[T](using m: Mirror.Of[T]) as Encoder[T] = {

    inline m match {
      case s: Mirror.SumOf[T]     => println("SumOf")
      case p: Mirror.ProductOf[T] => println("ProductOf")
    }
    
    new Encoder[T] {
      override def encode(value: T, schema: Schema): AvroValue =
        AvroValue.AvroString("foo")
    }
  }
}

def output[T](t: T)(using encoder: Encoder[T]): Unit = {
  println(encoder.encode(t, null))
}


object happyBirthday extends App {
  val foo = Foo("qwe", 123)
  output(foo)
}

case class Foo(val a: String, val b: Int)