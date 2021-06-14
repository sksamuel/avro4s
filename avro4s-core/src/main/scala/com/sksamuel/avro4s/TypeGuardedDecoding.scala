package com.sksamuel.avro4s

import org.apache.avro.Schema

import java.nio.ByteBuffer
import org.apache.avro.generic.{GenericContainer, GenericFixed}
import org.apache.avro.util.Utf8

import java.util.UUID

trait TypeGuardedDecoding[T] extends Serializable {
  def guard(schema: Schema): PartialFunction[Any, Boolean]
}

object TypeGuardedDecoding {

  def apply[T](using g: TypeGuardedDecoding[T]): TypeGuardedDecoding[T] = g

  given TypeGuardedDecoding[String] = new TypeGuardedDecoding[String] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Utf8 => true
      case v: String => true
    }

  given TypeGuardedDecoding[Boolean] = new TypeGuardedDecoding[Boolean] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Boolean => true
    }

  given TypeGuardedDecoding[Double] = new TypeGuardedDecoding[Double] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Double => true
      case v: Float => true
    }

  given TypeGuardedDecoding[Float] = new TypeGuardedDecoding[Float] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Float => true
    }

  given TypeGuardedDecoding[Long] = new TypeGuardedDecoding[Long] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Long => true
      case v: Int => true
      case v: Short => true
      case v: Byte => true
    }

  given TypeGuardedDecoding[Int] = new TypeGuardedDecoding[Int] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Int => true
    }

  given TypeGuardedDecoding[UUID] = new TypeGuardedDecoding[UUID] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Utf8 => true
      case v: String => true
    }

  given[T]: TypeGuardedDecoding[Map[String, T]] = new TypeGuardedDecoding[Map[String, T]] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: java.util.Map[_, _] => true
    }

  given TypeGuardedDecoding[Array[Byte]] = new TypeGuardedDecoding[Array[Byte]] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: ByteBuffer => true
      case v: Array[Byte] => true
      case v: GenericFixed => true
    }

  given TypeGuardedDecoding[ByteBuffer] = new TypeGuardedDecoding[ByteBuffer] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: ByteBuffer => true
      case v: Array[Byte] => true
      case v: GenericFixed => true
    }

  given[T]: TypeGuardedDecoding[List[T]] = new TypeGuardedDecoding[List[T]] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Array[_] => true
      case v: java.util.Collection[_] => true
      case v: Iterable[_] => true
    }

  given[T]: TypeGuardedDecoding[Seq[T]] = new TypeGuardedDecoding[Seq[T]] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Array[_] => true
      case v: java.util.Collection[_] => true
      case v: Iterable[_] => true
    }

  given[T]: TypeGuardedDecoding[T] = new TypeGuardedDecoding[T] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: GenericContainer if v.getSchema.getFullName == schema.getFullName => true
    }
}