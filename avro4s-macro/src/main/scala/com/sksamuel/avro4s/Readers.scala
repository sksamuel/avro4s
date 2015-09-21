package com.sksamuel.avro4s

import org.apache.avro.generic.GenericRecord

import scala.reflect.macros.Context

trait AvroPopulator[T] {
  def read(record: GenericRecord): T
}

trait AvroFieldGetter[T] {
  def read(record: GenericRecord, name: String): T
}

trait AvroConverter[T] {
  def convert(value: AnyRef): T
}

object Readers {

  implicit val StringConverter = new AvroConverter[String] {
    override def convert(value: AnyRef): String = value.toString // should be org.apache.avro.util.Utf8
  }

  implicit val FloatConverter = new AvroConverter[Float] {
    override def convert(value: AnyRef): Float = value.toString.toFloat
  }

  implicit val LongConverter = new AvroConverter[Long] {
    override def convert(value: AnyRef): Long = value.toString.toLong
  }

  implicit val IntConverter = new AvroConverter[Int] {
    override def convert(value: AnyRef): Int = value.toString.toInt
  }

  implicit val BooleanConverter = new AvroConverter[Boolean] {
    override def convert(value: AnyRef): Boolean = value.toString.toBoolean
  }

  implicit val DoubleConverter = new AvroConverter[Double] {
    override def convert(value: AnyRef): Double = value.toString.toDouble
  }

  implicit def SeqConverter[S <: AnyRef](implicit converter: AvroConverter[S]) = new AvroConverter[Seq[S]] {
    override def convert(value: AnyRef): Seq[S] = {
      import scala.collection.JavaConverters._
      value.asInstanceOf[java.util.Collection[S]].asScala.toList.map(s => converter.convert(s))
    }
  }

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
      q"""{ import Readers._
            val converter = implicitly[AvroConverter[$sig]]
            val value = r.get($decoded)
            converter.convert(value)
          }
      """
    }

    c.Expr[AvroPopulator[T]]( q"""
      new AvroPopulator[$t] {
        import Readers._
        override def read(r: org.apache.avro.generic.GenericRecord): $t = {
          val t = new $t(..$params)
          t
        }
      }
    """)
  }
}
