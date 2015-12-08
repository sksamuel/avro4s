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

  implicit val BooleanConverter = new AvroConverter[Boolean] {
    override def convert(value: Any): Boolean = value.toString.toBoolean
  }

  implicit val StringConverter = new AvroConverter[String] {
    override def convert(value: Any): String = value.toString // should be org.apache.avro.util.Utf8
  }

  implicit val FloatConverter = new AvroConverter[Float] {
    override def convert(value: Any): Float = value.toString.toFloat
  }

  implicit val LongConverter = new AvroConverter[Long] {
    override def convert(value: Any): Long = value.toString.toLong
  }

  implicit val IntConverter = new AvroConverter[Int] {
    override def convert(value: Any): Int = value.toString.toInt
  }

  implicit val DoubleConverter = new AvroConverter[Double] {
    override def convert(value: Any): Double = value.toString.toDouble
  }

  implicit def OptionConverter[T](implicit converter: AvroConverter[T]) = new AvroConverter[Option[T]] {
    override def convert(value: Any): Option[T] = {
      Option(value).map(converter.convert)
    }
  }

  implicit def EitherConverter[L, R](implicit leftConverter: AvroConverter[L],
                                     rightConverter: AvroConverter[R],
                                     leftType: ClassTag[L],
                                     rightType: ClassTag[R]) = new AvroConverter[Either[L, R]] {
    def isMatch[T](tag: ClassTag[T], value: Any) = false
    override def convert(value: Any): Either[L, R] = {
      val name = value match {
        case utf8: Utf8 => classOf[String].getCanonicalName
        case true | false => classOf[Boolean].getCanonicalName
        case _: Int => classOf[Int].getCanonicalName
        case _: Long => classOf[Long].getCanonicalName
        case _: Double => classOf[Double].getCanonicalName
        case _: Float => classOf[Float].getCanonicalName
        case _ => value.getClass.getCanonicalName
      }
      if (leftType.runtimeClass.getCanonicalName == name) Left(leftConverter.convert(value))
      else if (rightType.runtimeClass.getCanonicalName == name) Right(rightConverter.convert(value))
      else throw new IllegalArgumentException(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
    }
  }

  implicit def SeqConverter[S](implicit converter: AvroConverter[S]) = new AvroConverter[Seq[S]] {
    override def convert(value: Any): Seq[S] = {
      import scala.collection.JavaConverters._
      value.asInstanceOf[java.util.Collection[S]].asScala.map(s => converter.convert(s)).toList
    }
  }

  implicit def SetConverter[S](implicit converter: AvroConverter[S]) = new AvroConverter[Set[S]] {
    override def convert(value: Any): Set[S] = {
      import scala.collection.JavaConverters._
      value.asInstanceOf[java.util.Collection[S]].asScala.map(s => converter.convert(s)).toSet
    }
  }

  implicit def MapConverter[S](implicit converter: AvroConverter[S]) = new AvroConverter[Map[String, S]] {
    override def convert(value: Any): Map[String, S] = {
      import scala.collection.JavaConverters._
      value.asInstanceOf[java.util.Map[String, S]].asScala.map { case (k, v) => k -> converter.convert(v) }.toMap
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
