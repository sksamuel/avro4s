package com.sksamuel.avro4s.internal

import org.apache.avro.generic.IndexedRecord

import scala.language.experimental.macros

// to be renamed back to [FromRecord]
trait RecordDecoder[T] {
  def decode(record: IndexedRecord): T
}

object RecordDecoder {

  implicit def apply[T]: RecordDecoder[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[RecordDecoder[T]] = {

    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe

    val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((f, fieldTpe), index) =>
      val name = f.name.asInstanceOf[c.TermName]
      // q"""values.append(t.$name : $fieldTpe)"""
    }

    c.Expr[RecordDecoder[T]](
      q"""
          new _root_.com.sksamuel.avro4s.internal.RecordDecoder[$tpe] {
            override def decode(record: _root_.org.apache.avro.generic.IndexedRecord): $tpe = {
              null
            }
          }
       """
    )
  }
}
