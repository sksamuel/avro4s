package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import shapeless.{:+:, CNil, Coproduct, Inr, Lazy}

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

// turns an avro value into a scala value
// type T is the target scala type
trait FromValue[T] {
  def apply(value: Any, field: Field = null): T
}

trait LowPriorityFromValue {
  implicit def apply[T](implicit fromRecord: FromRecord[T]): FromValue[T] = new FromValue[T] {
    override def apply(value: Any, field: Field): T = value match {
      case record: GenericRecord => fromRecord(record)
    }
  }
}

object FromValue extends LowPriorityFromValue {

  implicit object BigDecimalFromValue extends FromValue[BigDecimal] {
    override def apply(value: Any, field: Field): BigDecimal = BigDecimal(new String(value.asInstanceOf[ByteBuffer].array))
  }

  implicit object BooleanFromValue extends FromValue[Boolean] {
    override def apply(value: Any, field: Field): Boolean = value.toString.toBoolean
  }

  implicit object ByteArrayFromValue extends FromValue[Array[Byte]] {
    override def apply(value: Any, field: Field): Array[Byte] = value.asInstanceOf[ByteBuffer].array
  }

  implicit object DoubleFromValue extends FromValue[Double] {
    override def apply(value: Any, field: Field): Double = value.toString.toDouble
  }

  implicit object FloatFromValue extends FromValue[Float] {
    override def apply(value: Any, field: Field): Float = value.toString.toFloat
  }

  implicit object IntFromValue extends FromValue[Int] {
    override def apply(value: Any, field: Field): Int = value.toString.toInt
  }

  implicit object LongFromValue extends FromValue[Long] {
    override def apply(value: Any, field: Field): Long = value.toString.toLong
  }

  implicit object StringFromValue extends FromValue[String] {
    override def apply(value: Any, field: Field): String = value.toString
  }

  implicit object UUIDFromValue extends FromValue[UUID] {
    override def apply(value: Any, field: Field): UUID = UUID.fromString(value.toString)
  }

  implicit def OptionFromValue[T](implicit fromvalue: FromValue[T]) = new FromValue[Option[T]] {
    override def apply(value: Any, field: Field): Option[T] = Option(value).map((value: Any) => fromvalue.apply(value))
  }

  implicit def JavaEnumFromValue[E <: Enum[E]](implicit tag: ClassTag[E]) = new FromValue[E] {
    override def apply(value: Any, field: Field): E = Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], value.toString)
  }

  implicit def ScalaEnumFromValue[E <: Enumeration#Value] = new FromValue[E] {
    override def apply(value: Any, field: Field): E = {
      val klass = Class.forName(field.schema.getFullName + "$")
      import scala.reflect.NameTransformer._
      val enum = klass.getField(MODULE_INSTANCE_NAME).get(null).asInstanceOf[Enumeration]
      enum.withName(value.toString).asInstanceOf[E]
    }
  }

  implicit def VectorFromValue[T](implicit fromvalue: FromValue[T]): FromValue[Vector[T]] = new FromValue[Vector[T]] {
    override def apply(value: Any, field: Field): Vector[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value)).toVector
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toVector
      case other => sys.error("Unsupported vector " + other)
    }
  }

  implicit def ArrayFromValue[T](implicit fromvalue: FromValue[T],
                                 tag: ClassTag[T]): FromValue[Array[T]] = new FromValue[Array[T]] {
    override def apply(value: Any, field: Field): Array[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value))
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toArray
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def SetFromValue[T](implicit fromvalue: FromValue[T]): FromValue[Set[T]] = new FromValue[Set[T]] {
    override def apply(value: Any, field: Field): Set[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value)).toSet
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toSet
      case other => sys.error("Unsupported set " + other)
    }
  }

  implicit def ListFromValue[T](implicit fromvalue: FromValue[T]): FromValue[List[T]] = new FromValue[List[T]] {
    override def apply(value: Any, field: Field): List[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value)).toList
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toList
      case other => sys.error("Unsupported list " + other)
    }
  }

  implicit def MapFromValue[T](implicit fromvalue: FromValue[T]): FromValue[Map[String, T]] = new FromValue[Map[String, T]] {
    override def apply(value: Any, field: Field): Map[String, T] = value match {
      case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> fromvalue(v) }
      case other => sys.error("Unsupported map " + other)
    }
  }

  implicit def SeqFromValue[T](implicit fromvalue: FromValue[T]): FromValue[Seq[T]] = new FromValue[Seq[T]] {
    override def apply(value: Any, field: Field): Seq[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value))
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toList
      case other => sys.error("Unsupported seq " + other)
    }
  }

  import scala.reflect.runtime.universe.WeakTypeTag

  private def safeFrom[T: WeakTypeTag : FromValue](value: Any): Option[T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe
    val from = implicitly[FromValue[T]]

    def typeName: String = {
      val nearestPackage = Stream.iterate(tpe.typeSymbol.owner)(_.owner).dropWhile(!_.isPackage).head
      s"${nearestPackage.fullName}.${tpe.typeSymbol.name.decodedName}"
    }

    value match {
      case utf8: Utf8 if tpe <:< typeOf[java.lang.String] => Some(from(value))
      case true | false if tpe <:< typeOf[Boolean] => Some(from(value))
      case _: Int if tpe <:< typeOf[Int] => Some(from(value))
      case _: Long if tpe <:< typeOf[Long] => Some(from(value))
      case _: Double if tpe <:< typeOf[Double] => Some(from(value))
      case _: Float if tpe <:< typeOf[Float] => Some(from(value))
      // we don't need to worry about the inner type of the array,
      // as avro schemas will not legally allow multiple arrays in a union
      // tpe is the type we're _expecting_, though, so we need to
      // check both scala and java collections
      case _: GenericData.Array[_]
        if tpe <:< typeOf[Array[_]] ||
          tpe <:< typeOf[java.util.Collection[_]] ||
          tpe <:< typeOf[Iterable[_]] =>
        Some(from(value))
      // and similarly for maps
      case _: java.util.Map[_, _]
        if tpe <:< typeOf[java.util.Map[_, _]] ||
          tpe <:< typeOf[Map[_, _]] =>
        Some(from(value))
      case record: GenericData.Record if typeName == record.getSchema.getFullName => Some(from(value))
      case _ => None
    }
  }

  private def errorString(value: Any, field: Field) = {
    val klass = value match {
      case null => "null"
      case _ => value.getClass.toString
    }

    val fieldName = field match {
      case null => "[unknown]"
      case _ => s"[${field.name}]"
    }

    s"Value $value of type $klass is not compatible with $fieldName"
  }

  implicit def EitherFromValue[A: WeakTypeTag : FromValue, B: WeakTypeTag : FromValue]: FromValue[Either[A, B]] = new FromValue[Either[A, B]] {
    override def apply(value: Any, field: Field): Either[A, B] =
      safeFrom[A](value).map(Left[A, B](_))
        .orElse(safeFrom[B](value).map(Right[A, B](_)))
        .getOrElse(sys.error(errorString(value, field)))
  }

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))

  // `apply` here should never be invoked under normal operation; if
  // we're trying to read a value of type CNil it's because we've
  // tried all the other cases and failed. But the FromValue[CNil]
  // needs to exist to supply a base case for the recursion.
  implicit def CNilFromValue: FromValue[CNil] = new FromValue[CNil] {
    override def apply(value: Any, field: Field): CNil = sys.error(errorString(value, field))
  }

  // We're expecting to read a value of type S :+: T from avro.  Avro
  // unions are untyped, so we have to attempt to read a value of type
  // S (the concrete type), and if that fails, attempt to read the
  // rest of the coproduct type T.

  // thus, the bulk of the logic here is shared with reading Eithers, in `safeFrom`.
  implicit def CoproductFromValue[S: WeakTypeTag : FromValue, T <: Coproduct : FromValue]: FromValue[S :+: T] = new FromValue[S :+: T] {
    override def apply(value: Any, field: Field): S :+: T =
      safeFrom[S](value).map(Coproduct[S :+: T](_))
        .getOrElse(Inr(implicitly[FromValue[T]].apply(value, field)))
  }
}

// converts an avro record into a type T
trait FromRecord[T] extends Serializable {
  def apply(record: org.apache.avro.generic.GenericRecord): T
}

object FromRecord {

  implicit def apply[T]: FromRecord[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[FromRecord[T]] = {
    import c.universe._
    val tpe = weakTypeTag[T].tpe
    require(tpe.typeSymbol.asClass.isCaseClass, s"Require a case class but $tpe is not")

    def fieldsForType(tpe: c.universe.Type): List[c.universe.Symbol] = {
      tpe.decls.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m.paramLists.head
      }.getOrElse(Nil)
    }

    val companion = tpe.typeSymbol.companion

    val converters: Seq[Tree] = fieldsForType(tpe).map { f =>
      val sig = f.typeSignature
      val fixedAnnotation: Option[AvroFixed] = sig.typeSymbol.annotations.collectFirst {
        case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
          anno.tree.children.tail match {
            case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
          }
      }

      fixedAnnotation match {
        case Some(fixed) =>
          q"""{
            null
          }
          """
        case None =>
          q"""com.sksamuel.avro4s.FromRecord.lazyConverter[$sig]"""
      }
    }

    val fromValues: Seq[Tree] = fieldsForType(tpe).zipWithIndex.map {
      case (f, idx) =>
        val name = f.name.asInstanceOf[c.TermName]
        val decoded: String = name.decodedName.toString
        val sig = f.typeSignature
        val fixedAnnotation: Option[AvroFixed] = sig.typeSymbol.annotations.collectFirst {
          case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
            anno.tree.children.tail match {
              case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
            }
        }

        val valueClass = sig.typeSymbol.isClass && sig.typeSymbol.asClass.isDerivedValueClass
        if (fixedAnnotation.nonEmpty) {
          q"""
          {
            val value = record.get($decoded).asInstanceOf[org.apache.avro.generic.GenericData.Fixed]
            new $sig(new scala.collection.mutable.WrappedArray.ofByte(value.bytes()))
          }
          """
        } else if (valueClass) {
          val valueCstr = sig.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
          val valueFieldType = valueCstr.typeSignature

          // the name of the field is always the outer field, ie the name of the variable
          // that refers to the value class itself, and not the variable inside the value class
          q"""
          {
            val converter = com.sksamuel.avro4s.FromRecord.lazyConverter[$valueFieldType]
            val value = converter.value(record.get($decoded), record.getSchema.getField($decoded))
            new $sig(value)
          }
          """
        } else {
          q"""
          {
            val converter = converters($idx).asInstanceOf[shapeless.Lazy[com.sksamuel.avro4s.FromValue[$sig]]]
            converter.value(record.get($decoded), record.getSchema.getField($decoded))
          }
          """
        }
    }

    c.Expr[FromRecord[T]](
      q"""new com.sksamuel.avro4s.FromRecord[$tpe] {
            private val converters: Array[shapeless.Lazy[com.sksamuel.avro4s.FromValue[_]]] = Array(..$converters)

            def apply(record: org.apache.avro.generic.GenericRecord): $tpe = {
              $companion.apply(..$fromValues)
            }
          }
        """
    )
  }

  def lazyConverter[T](implicit fromValue: Lazy[FromValue[T]]): Lazy[FromValue[T]] = fromValue
}
