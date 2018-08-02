package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.sksamuel.avro4s.ToSchema.defaultScaleAndPrecisionAndRoundingMode
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Conversions, LogicalTypes, Schema}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{:+:, CNil, Coproduct, Generic, HList, Inl, Inr, Lazy}

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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
}

object LowPriorityToValue {

  def fixedImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[ToValue[T]] = {
    import c.universe._
    val tpe = weakTypeTag[T].tpe
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

  implicit object LocalDateToValue extends ToValue[LocalDate] {
    override def apply(value: LocalDate): String = value.format(DateTimeFormatter.ISO_LOCAL_DATE)
  }

  implicit object LocalDateTimeToValue extends ToValue[LocalDateTime] {
    override def apply(value: LocalDateTime): String = value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  }

  implicit def BigDecimalToValue(implicit sp: ScaleAndPrecisionAndRoundingMode = defaultScaleAndPrecisionAndRoundingMode): ToValue[BigDecimal] = {
    val decimalConversion = new Conversions.DecimalConversion
    val decimalType = LogicalTypes.decimal(sp.precision, sp.scale)
    new ToValue[BigDecimal] {
      override def apply(value: BigDecimal): ByteBuffer = {
        val scaledValue = value.setScale(sp.scale, sp.roundingMode)
        decimalConversion.toBytes(scaledValue.bigDecimal, null, decimalType)
      }
    }
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

  implicit object ByteToValue extends ToValue[Byte] {
    override def apply(value: Byte) = value.toInt
  }

  implicit object ShortToValue extends ToValue[Short] {
    override def apply(value: Short) = value.toInt
  }

  implicit object ByteArrayToValue extends ToValue[Array[Byte]] {
    override def apply(value: Array[Byte]): ByteBuffer = ByteBuffer.wrap(value)
  }

  implicit object ByteSeqToValue extends ToValue[Seq[Byte]] {
    override def apply(value: Seq[Byte]): ByteBuffer = ByteBuffer.wrap(value.toArray[Byte])
  }

  implicit def MapToValue[T](implicit tovalue: ToValue[T]) = new ToValue[Map[String, T]] {
    override def apply(value: Map[String, T]): java.util.Map[String, T] = {
      value.mapValues(tovalue.apply).asInstanceOf[Map[String, T]].asJava
    }
  }

  implicit def JavaEnumToValue[E <: Enum[_]](implicit toSchema: ToSchema[E]): ToValue[E] = new ToValue[E] {
    override def apply(value: E): Any = new EnumSymbol(toSchema(), value)
  }

  implicit def ScalaEnumToValue[E <: Enumeration#Value](implicit toSchema: ToSchema[E]): ToValue[E] = new ToValue[E] {
    override def apply(value: E): Any = new EnumSymbol(toSchema(), value.toString)
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

  implicit def genTraitObjectEnum[T, C <: Coproduct, L <: HList](implicit ct: ClassTag[T], gen: Generic.Aux[T, C],
                                                     objs: Reify.Aux[C, L],toList: ToList[L, T]): ToValue[T] = new ToValue[T] {

    protected val schema: Schema = {
      val tpe = weakTypeTag[T]
      val namespace = tpe.tpe.typeSymbol.annotations.map(_.toString)
        .find(_.startsWith("com.sksamuel.avro4s.AvroNamespace"))
        .map(_.stripPrefix("com.sksamuel.avro4s.AvroNamespace(\"").stripSuffix("\")"))
        .getOrElse(ct.runtimeClass.getPackage.getName)
      val name = ct.runtimeClass.getSimpleName
      val symbols = toList(objs()).map(_.toString).asJava
      Schema.createEnum(name, null, namespace, symbols)
    }

    override def apply(value: T): Any = new EnumSymbol(schema, value.toString)
  }
}

trait ToRecord[T] extends Serializable {
  def apply(t: T): GenericRecord
}

object ToRecord {

  implicit def apply[T]: ToRecord[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[ToRecord[T]] = {
    import c.universe._
    val helper = TypeHelper(c)
    val tpe = weakTypeTag[T].tpe

    val constructorArgumentsWithTypes = helper.fieldsOf(tpe)
    val converters: Seq[Tree] = constructorArgumentsWithTypes.map { case (sym, sig) =>
      helper.fixed(sig.typeSymbol) match {
        case Some(AvroFixed(_)) => q"""{_root_.shapeless.Lazy(com.sksamuel.avro4s.ToValue.fixed[$sig])}"""
        case None => q"""_root_.com.sksamuel.avro4s.ToRecord.lazyConverter[$sig]"""
      }
    }

    val puts: Seq[Tree] = constructorArgumentsWithTypes.zipWithIndex.map {
      case ((sym, sig), idx) =>

        val name = sym.name.asInstanceOf[c.TermName]
        val fieldName = helper.avroName(sym).getOrElse(q"${name.decodedName.toString}")
        val valueClass = sig.typeSymbol.isClass && sig.typeSymbol.asClass.isDerivedValueClass
        val typeFixed = helper.fixed(sig.typeSymbol)
        val fieldFixed = helper.fixed(sym)

        // if the field has been annotated with fixed then we need to wrap it in a FixedToValue
        if (typeFixed.nonEmpty) {
          q"""
          {
            val converter = converters($idx).asInstanceOf[_root_.shapeless.Lazy[_root_.com.sksamuel.avro4s.ToValue[$sig]]]
            record.put($fieldName, converter.value(t.$name : $sig))
          }
          """
          // else if the field has been annotated with fixed, then we need to convert
          // the field value into a byte array
        } else if (fieldFixed.nonEmpty) {
          val size = fieldFixed.get.size
          q"""
          {
             val schema = _root_.org.apache.avro.SchemaBuilder.fixed($fieldName).size($size)
             val f = new _root_.org.apache.avro.generic.GenericData.Fixed(schema, t.$name.getBytes("UTF-8").array)
             record.put($fieldName, f)
          }
          """
          // if a field is a value class we need to handle it here, using a converter
          // for the underlying value rather than the actual value class
        } else if (valueClass) {
          val valueCstr = sig.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
          val valueFieldType = valueCstr.typeSignature
          val valueFieldName = valueCstr.name.asInstanceOf[c.TermName]
          q"""
          {
            val converter = _root_.com.sksamuel.avro4s.ToRecord.lazyConverter[$valueFieldType]
            record.put($fieldName, converter.value(t.$name.$valueFieldName : $valueFieldType))
          }
          """
        } else {
          q"""
          {
            val converter = converters($idx).asInstanceOf[_root_.shapeless.Lazy[_root_.com.sksamuel.avro4s.ToValue[$sig]]]
            record.put($fieldName, converter.value(t.$name : $sig))
          }
          """
        }
    }

    c.Expr[ToRecord[T]](
      q"""new _root_.com.sksamuel.avro4s.ToRecord[$tpe] {
            private val schemaFor : _root_.com.sksamuel.avro4s.SchemaFor[$tpe] = _root_.com.sksamuel.avro4s.SchemaFor[$tpe]
            private val converters : Array[_root_.shapeless.Lazy[_root_.com.sksamuel.avro4s.ToValue[_]]] = Array(..$converters)

            def apply(t : $tpe): _root_.org.apache.avro.generic.GenericRecord = {

              val record = new _root_.org.apache.avro.generic.GenericData.Record(schemaFor())
              ..$puts
              record
            }
          }"""
    )
  }

  def lazyConverter[T](implicit toValue: Lazy[ToValue[T]]): Lazy[ToValue[T]] = toValue
}
