package com.sksamuel.avro4s.internal

import org.apache.avro.Schema

import scala.language.experimental.macros
import scala.language.implicitConversions

// to be renamed back to [ToRecord]
trait RecordEncoder[T] {
  def encode(t: T): InternalRecord
}

object RecordEncoder {

  implicit def apply[T](schema: Schema): RecordEncoder[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context)(schema: c.Tree): c.Expr[RecordEncoder[T]] = {

    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe
    val annos = reflect.annotations(tpe.typeSymbol)
    val extractor = new AnnotationExtractors(annos)

    val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((f, fieldTpe), index) =>

      val name = f.name.asInstanceOf[c.TermName]
      val annos = reflect.annotations(tpe.typeSymbol)
      val extractor = new AnnotationExtractors(annos)

      extractor.fixed match {
        case Some(fixed) =>
          q"""
          {
             val fixedSchema = _root_.org.apache.avro.SchemaBuilder.fixed($name).size($fixed)
             val fixed = new _root_.org.apache.avro.generic.GenericData.Fixed(fixedSchema, t.$name.getBytes("UTF-8").array)
             values.append(fixed : $fieldTpe)
          }
          """
        case None =>
          q"""values.append(t.$name : $fieldTpe)"""
      }
    }

    c.Expr[RecordEncoder[T]](
      q"""
          new _root_.com.sksamuel.avro4s.internal.RecordEncoder[$tpe] {
            override def encode(t: $tpe): _root_.com.sksamuel.avro4s.internal.InternalRecord = {
              val values = _root_.scala.collection.mutable.ListBuffer.empty[Any]
              ..$fields
              new _root_.com.sksamuel.avro4s.internal.InternalRecord($schema, values.toVector)
            }
          }
       """
    )
  }
}
