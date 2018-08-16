package com.sksamuel.avro4s.internal

import scala.language.experimental.macros

trait RecordEncoder[T] {
  def encode(t: T): Record
}

object RecordEncoder {

  implicit def apply[T]: RecordEncoder[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[RecordEncoder[T]] = {

    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe

    c.Expr[RecordEncoder[T]](
      q"""
          new _root_.com.sksamuel.avro4s.internal.RecordEncoder[$tpe] {
            private val schema = _root_.com.sksamuel.avro4s.internal.SchemaEncoder[$tpe].encode
            override def encode(t: $tpe): _root_.com.sksamuel.avro4s.internal.Record = new _root_.com.sksamuel.avro4s.internal.Record(schema)
          }
       """
    )
  }
}
