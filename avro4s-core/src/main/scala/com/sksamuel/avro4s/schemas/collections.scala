package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.Schema

trait CollectionSchemas:

  given [T](using schemaFor: SchemaFor[T]) : SchemaFor[Array[T]] = new SchemaFor[Array[T]] {
    override def schema[T]: Schema = Schema.createArray(schemaFor.schema)
  }

//  implicit def iterableSchemaFor[T](implicit item: SchemaFor[T]): SchemaFor[Iterable[T]] =
//    _iterableSchemaFor[Iterable, T](item)
//
//  implicit def listSchemaFor[T](implicit item: SchemaFor[T]): SchemaFor[List[T]] =
//    _iterableSchemaFor[List, T](item)
//
//  implicit def setSchemaFor[T](implicit item: SchemaFor[T]): SchemaFor[Set[T]] =
//    _iterableSchemaFor[Set, T](item)
//
//  implicit def vectorSchemaFor[T](implicit item: SchemaFor[T]): SchemaFor[Vector[T]] =
//    _iterableSchemaFor[Vector, T](item)
//
//  implicit def seqSchemaFor[T](implicit item: SchemaFor[T]): SchemaFor[Seq[T]] =
//    _iterableSchemaFor[Seq, T](item)
//  
