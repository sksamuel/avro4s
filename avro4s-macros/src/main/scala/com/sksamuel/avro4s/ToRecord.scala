package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.GenericRecord
import shapeless.ops.coproduct.Reify
import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr, Lazy}

import scala.collection.JavaConverters._
import scala.language.experimental.macros

trait ToValue[A] {
  def apply(value: A): Any = value
}

trait LowPriorityToValue {

  implicit def genCoproduct[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
                                               coproductToValue: ToValue[C]): ToValue[T] = new ToValue[T] {
    override def apply(value: T): Any = coproductToValue(gen.to(value))
  }

  implicit def apply[T](implicit toRecord: ToRecord[T]): ToValue[T] = new ToValue[T] {
    override def apply(value: T): GenericRecord = toRecord(value)
  }

  def fixed[T]: ToValue[T] = macro LowPriorityToValue.fixedImpl[T]

  def fixedWithSchemaFor[T](schemaFor: SchemaFor[T]): ToValue[T] = macro LowPriorityToValue.fixedImplWithSchemaFor[T]
}

object LowPriorityToValue {
  def fixedImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[ToValue[T]] = {
    import c.universe._
    val tpe = weakTypeTag[T].tpe

    val fixedAnnotation: Option[AvroFixed] = tpe.typeSymbol.annotations.collectFirst {
      case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
        anno.tree.children.tail match {
          case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
        }
    }

    c.Expr[ToValue[T]](
      q"""
        {
          val schema = com.sksamuel.avro4s.SchemaFor[$tpe]()
          new com.sksamuel.avro4s.ToValue[$tpe] {
            override def apply(t: $tpe): org.apache.avro.generic.GenericFixed = {
              new org.apache.avro.generic.GenericData.Fixed(schema, t.bytes.array)
            }
          }
        }
      """)
  }

  def fixedImplWithSchemaFor[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context)(schemaFor: c.Expr[SchemaFor[T]]): c.Expr[ToValue[T]] = {
    import c.universe._
    val tpe = weakTypeTag[T].tpe

    val fixedAnnotation: Option[AvroFixed] = tpe.typeSymbol.annotations.collectFirst {
      case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
        anno.tree.children.tail match {
          case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
        }
    }

    c.Expr[ToValue[T]](
      q"""
        {
          val schema = $schemaFor()
          new com.sksamuel.avro4s.ToValue[$tpe] {
            override def apply(t: $tpe): org.apache.avro.generic.GenericFixed = {
              new org.apache.avro.generic.GenericData.Fixed(schema, t.bytes.array)
            }
          }
        }
      """)
  }
}

object ToValue extends LowPriorityToValue {

  implicit object BooleanToValue extends ToValue[Boolean]

  implicit object StringToValue extends ToValue[String]

  implicit object DoubleToValue extends ToValue[Double]

  implicit object FloatToValue extends ToValue[Float]

  implicit object IntToValue extends ToValue[Int]

  implicit object LongToValue extends ToValue[Long]

  implicit object UUIDToValue extends ToValue[UUID] {
    override def apply(value: UUID): String = value.toString
  }

  implicit object BigDecimalToValue extends ToValue[BigDecimal] {
    override def apply(value: BigDecimal): ByteBuffer = ByteBuffer.wrap(value.toString.getBytes)
  }

  implicit def ListToValue[T](implicit tovalue: ToValue[T]): ToValue[List[T]] = new ToValue[List[T]] {
    override def apply(values: List[T]): Any = values.map(tovalue.apply).asJava
  }

  implicit def SetToValue[T](implicit tovalue: ToValue[T]): ToValue[Set[T]] = new ToValue[Set[T]] {
    override def apply(values: Set[T]): Any = values.map(tovalue.apply).asJava
  }

  implicit def VectorToValue[T](implicit tovalue: ToValue[T]): ToValue[Vector[T]] = new ToValue[Vector[T]] {
    override def apply(values: Vector[T]): Any = values.map(tovalue.apply).asJava
  }

  implicit def SeqToValue[T](implicit tovalue: ToValue[T]): ToValue[Seq[T]] = new ToValue[Seq[T]] {
    override def apply(values: Seq[T]): Any = values.map(tovalue.apply).asJava
  }

  implicit def OptionToValue[T](implicit tovalue: ToValue[T]) = new ToValue[Option[T]] {
    override def apply(value: Option[T]): Any = value.map(tovalue.apply).orNull
  }

  implicit def ArrayToValue[T](implicit tovalue: ToValue[T]): ToValue[Array[T]] = new ToValue[Array[T]] {
    override def apply(value: Array[T]): Any = value.headOption match {
      case Some(b: Byte) => ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
      case _ => value.map(tovalue.apply).toSeq.asJavaCollection
    }
  }

  implicit object ByteArrayToValue extends ToValue[Array[Byte]] {
    override def apply(value: Array[Byte]): ByteBuffer = ByteBuffer.wrap(value)
  }

  implicit def MapToValue[T](implicit tovalue: ToValue[T]) = new ToValue[Map[String, T]] {
    override def apply(value: Map[String, T]): java.util.Map[String, T] = {
      value.mapValues(tovalue.apply).asInstanceOf[Map[String, T]].asJava
    }
  }

  implicit def JavaEnumToValue[E <: Enum[_]]: ToValue[E] = new ToValue[E] {
    override def apply(value: E): Any = new EnumSymbol(null, value)
  }

  implicit def ScalaEnumToValue[E <: Enumeration#Value]: ToValue[E] = new ToValue[E] {
    override def apply(value: E): Any = new EnumSymbol(null, value.toString)
  }

  implicit def EitherToValue[T, U](implicit lefttovalue: ToValue[T], righttovalue: ToValue[U]) = new ToValue[Either[T, U]] {
    override def apply(value: Either[T, U]): Any = value match {
      case Left(left) => lefttovalue(left)
      case Right(right) => righttovalue(right)
    }
  }

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))

  // `apply` here should never actually be invoked, because you can't
  // actually construct a value of type a: CNil, but the ToValue[CNil]
  // needs to exist to supply a base case for the recursion.
  implicit def CNilToValue: ToValue[CNil] = new ToValue[CNil] {
    override def apply(value: CNil): Any = sys.error("This should never happen: CNil has no inhabitants")
  }

  // A :+: B is either Inl(value: A) or Inr(value: B), continuing the recursion
  implicit def CoproductToValue[S, T <: Coproduct](implicit curToValue: ToValue[S], restToValue: ToValue[T]): ToValue[S :+: T] = new ToValue[S :+: T] {
    override def apply(value: S :+: T): Any = value match {
      case Inl(s) => curToValue(s)
      case Inr(t) => restToValue(t)
    }
  }

  implicit def genTraitObjectEnum[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
                                                     objs: Reify[C]): ToValue[T] = new ToValue[T] {
    override def apply(value: T): Any = new EnumSymbol(null, value.toString)
  }
}

trait ToRecord[T] extends Serializable {
  def apply(t: T): GenericRecord
}

object ToRecord {

  implicit def apply[T]: ToRecord[T] = macro applyImpl[T]
  def withSchemaFor[T](schemaFor: SchemaFor[T]): ToRecord[T] = macro withSchemaForImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[ToRecord[T]] = {
    import c.universe._
    val helper = TypeHelper(c)
    val tpe = weakTypeTag[T].tpe

    val constructorArgumentsWithTypes = helper.fieldsOf(tpe)
    val converters: Seq[Tree] = constructorArgumentsWithTypes.map { case (sym, sig) =>

      val fixedAnnotation: Option[AvroFixed] = sig.typeSymbol.annotations.collectFirst {
        case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
          anno.tree.children.tail match {
            case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
          }
      }

      fixedAnnotation match {
        case Some(AvroFixed(size)) =>
          q"""{
            shapeless.Lazy(com.sksamuel.avro4s.ToValue.fixed[$sig])
          }
          """
        case None =>
          q"""com.sksamuel.avro4s.ToRecord.lazyConverter[$sig]"""
      }
    }

    val puts: Seq[Tree] = constructorArgumentsWithTypes.zipWithIndex.map {
      case ((f, sig), idx) =>
        val name = f.name.asInstanceOf[c.TermName]
        val fieldName: String = name.decodedName.toString
        val fixedAnnotation: Option[AvroFixed] = sig.typeSymbol.annotations.collectFirst {
          case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
            anno.tree.children.tail match {
              case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
            }
        }

        val valueClass = sig.typeSymbol.isClass && sig.typeSymbol.asClass.isDerivedValueClass

        // if a field is a value class we need to handle it here, using a converter
        // for the underlying value rather than the actual value class
        if (fixedAnnotation.nonEmpty) {
          q"""
          {
            val converter = converters($idx).asInstanceOf[shapeless.Lazy[com.sksamuel.avro4s.ToValue[$sig]]]
            record.put($fieldName, converter.value(t.$name : $sig))
          }
          """
        } else if (valueClass) {
          val valueCstr = sig.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
          val valueFieldType = valueCstr.typeSignature
          val valueFieldName = valueCstr.name.asInstanceOf[c.TermName]
          q"""
          {
            val converter = com.sksamuel.avro4s.ToRecord.lazyConverter[$valueFieldType]
            record.put($fieldName, converter.value(t.$name.$valueFieldName : $valueFieldType))
          }
          """
        } else {
          q"""
          {
            val converter = converters($idx).asInstanceOf[shapeless.Lazy[com.sksamuel.avro4s.ToValue[$sig]]]
            record.put($fieldName, converter.value(t.$name : $sig))
          }
          """
        }
    }

    c.Expr[ToRecord[T]](
      q"""new com.sksamuel.avro4s.ToRecord[$tpe] {
            private val schemaFor : com.sksamuel.avro4s.SchemaFor[$tpe] = com.sksamuel.avro4s.SchemaFor[$tpe]
            private val converters : Array[shapeless.Lazy[com.sksamuel.avro4s.ToValue[_]]] = Array(..$converters)

            def apply(t : $tpe): org.apache.avro.generic.GenericRecord = {

              val record = new org.apache.avro.generic.GenericData.Record(schemaFor())
              ..$puts
              record
            }
          }
        """
    )
  }

  def withSchemaForImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context)(schemaFor: c.Expr[SchemaFor[T]]): c.Expr[ToRecord[T]] = {
    import c.universe._
    val helper = TypeHelper(c)
    val tpe = weakTypeTag[T].tpe

    val constructorArgumentsWithTypes = helper.fieldsOf(tpe)
    val converters: Seq[Tree] = constructorArgumentsWithTypes.map { case (sym, sig) =>

      val fixedAnnotation: Option[AvroFixed] = sig.typeSymbol.annotations.collectFirst {
        case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
          anno.tree.children.tail match {
            case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
          }
      }

      fixedAnnotation match {
        case Some(AvroFixed(size)) =>
          q"""{
            shapeless.Lazy(com.sksamuel.avro4s.ToValue.fixedWithSchemaFor[$sig]($schemaFor))
          }
          """
        case None =>
          q"""com.sksamuel.avro4s.ToRecord.lazyConverter[$sig]"""
      }
    }

    val puts: Seq[Tree] = constructorArgumentsWithTypes.zipWithIndex.map {
      case ((f, sig), idx) =>
        val name = f.name.asInstanceOf[c.TermName]
        val fieldName: String = name.decodedName.toString
        val fixedAnnotation: Option[AvroFixed] = sig.typeSymbol.annotations.collectFirst {
          case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
            anno.tree.children.tail match {
              case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
            }
        }

        val valueClass = sig.typeSymbol.isClass && sig.typeSymbol.asClass.isDerivedValueClass

        // if a field is a value class we need to handle it here, using a converter
        // for the underlying value rather than the actual value class
        if (fixedAnnotation.nonEmpty) {
          q"""
          {
            val converter = converters($idx).asInstanceOf[shapeless.Lazy[com.sksamuel.avro4s.ToValue[$sig]]]
            record.put($fieldName, converter.value(t.$name : $sig))
          }
          """
        } else if (valueClass) {
          val valueCstr = sig.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
          val valueFieldType = valueCstr.typeSignature
          val valueFieldName = valueCstr.name.asInstanceOf[c.TermName]
          q"""
          {
            val converter = com.sksamuel.avro4s.ToRecord.lazyConverter[$valueFieldType]
            record.put($fieldName, converter.value(t.$name.$valueFieldName : $valueFieldType))
          }
          """
        } else {
          q"""
          {
            val converter = converters($idx).asInstanceOf[shapeless.Lazy[com.sksamuel.avro4s.ToValue[$sig]]]
            record.put($fieldName, converter.value(t.$name : $sig))
          }
          """
        }
    }

    c.Expr[ToRecord[T]](
      q"""new com.sksamuel.avro4s.ToRecord[$tpe] {
            private val schemaFor : com.sksamuel.avro4s.SchemaFor[$tpe] = $schemaFor
            private val converters : Array[shapeless.Lazy[com.sksamuel.avro4s.ToValue[_]]] = Array(..$converters)

            def apply(t : $tpe): org.apache.avro.generic.GenericRecord = {

              val record = new org.apache.avro.generic.GenericData.Record(schemaFor())
              ..$puts
              record
            }
          }
        """
    )
  }

  def lazyConverter[T](implicit toValue: Lazy[ToValue[T]]): Lazy[ToValue[T]] = toValue
}
