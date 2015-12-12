package com.sksamuel.avro4s

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.reflect.ClassTag
import scala.reflect.macros.Context

trait AvroPopulator[T] {
  def read(record: GenericRecord): T
}

trait AvroConverter[T] {
  def convert(value: Any): T
}

object Readers {



  def impl[T: c.WeakTypeTag](c: Context): c.Expr[AvroPopulator[T]] = {

    import c.universe._
    val t = weakTypeOf[T]

    val fields = t.declarations.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramss.head

    val params: Seq[Tree] = fields.map { f =>
      val termName = f.name.toTermName
      val decoded = f.name.decoded
      val sig = f.typeSignature
      q"""{ import com.sksamuel.avro4s.Readers._
            val converter = implicitly[com.sksamuel.avro4s.AvroConverter[$sig]]
            val value = r.get($decoded)
            converter.convert(value)
          }
      """
    }

    c.Expr[AvroPopulator[T]](
      q"""
      new com.sksamuel.avro4s.AvroPopulator[$t] {
        import com.sksamuel.avro4s.Readers._
        override def read(r: org.apache.avro.generic.GenericRecord): $t = {
          val t = new $t(..$params)
          t
        }
      }
    """)
  }
}
