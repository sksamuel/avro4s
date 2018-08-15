package com.sksamuel.avro4s.internal

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

trait DataTypeFor[T] {
  def dataType: DataType
}

object DataTypeFor {

  implicit def apply[T]: DataTypeFor[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: whitebox.Context): c.Expr[DataTypeFor[T]] = {

    import c.universe._

    val reflect = ReflectHelper(c)
    val tType = weakTypeOf[T]

    // we can only encode concrete classes as the top level
    require(tType.typeSymbol.isClass, tType + " is not a class but is " + tType.typeSymbol.fullName)

    val underlyingType = reflect.underlyingType(tType)

    val fields = reflect.fieldsOf(underlyingType).zipWithIndex.map { case ((f, fieldTpe), index) =>
      // the simple name of the field like "x"
      val fieldName = f.name.decodedName.toString.trim
      val annos = reflect.annotations(f)
      q"""{ _root_.com.sksamuel.avro4s.internal.DataTypeFor.structField[$fieldTpe]($fieldName, Seq(..$annos), null) }"""
    }

    // qualified name of the class we are building
    // todo encode generics:: + genericNameSuffix(underlyingType)
    val className = underlyingType.typeSymbol.fullName.toString

    // these are annotations on the class itself
    val annos = reflect.annotations(underlyingType.typeSymbol)

    c.Expr[DataTypeFor[T]](
      q"""
        new _root_.com.sksamuel.avro4s.internal.DataTypeFor[$tType] {
          val structType = _root_.com.sksamuel.avro4s.internal.StructType($className, Seq(..$annos), Seq(..$fields))
          override def dataType: com.sksamuel.avro4s.internal.DataType = structType
        }
      """
    )
  }

  def structField[B](name: String, annos: Seq[Anno], default: Any)(implicit dataTypeFor: DataTypeFor[B]): StructField = {
    StructField(name, dataTypeFor.dataType, annos, default)
  }

  implicit object ByteFor extends DataTypeFor[Byte] {
    override def dataType: DataType = ByteType
  }

  implicit object StringFor extends DataTypeFor[String] {
    override def dataType: DataType = StringType
  }

  implicit object LongFor extends DataTypeFor[Long] {
    override def dataType: DataType = LongType
  }

  implicit object IntFor extends DataTypeFor[Int] {
    override def dataType: DataType = IntType
  }

  implicit object DoubleFor extends DataTypeFor[Double] {
    override def dataType: DataType = DoubleType
  }

  implicit object FloatFor extends DataTypeFor[Float] {
    override def dataType: DataType = FloatType
  }

  implicit object BooleanFor extends DataTypeFor[Boolean] {
    override def dataType: DataType = BooleanType
  }

  implicit object ShortFor extends DataTypeFor[Short] {
    override def dataType: DataType = ShortType
  }

  implicit object UUIDFor extends DataTypeFor[UUID] {
    override def dataType: DataType = UUIDType
  }

  implicit object ByteArrayFor extends DataTypeFor[Array[Byte]] {
    override def dataType: DataType = BinaryType
  }

  implicit object ByteSeqFor extends DataTypeFor[Seq[Byte]] {
    override def dataType: DataType = BinaryType
  }

  implicit def OptionFor[T](implicit elementType: DataTypeFor[T]): DataTypeFor[Option[T]] = {
    new DataTypeFor[Option[T]] {
      override def dataType: DataType = UnionType(Seq(NullType, elementType.dataType))
    }
  }

  implicit def ArrayFor[S](implicit elementType: DataTypeFor[S]): DataTypeFor[Array[S]] = {
    new DataTypeFor[Array[S]] {
      override def dataType: DataType = ArrayType(elementType.dataType)
    }
  }

  implicit def IterableFor[S](implicit elementType: DataTypeFor[S]): DataTypeFor[Iterable[S]] = {
    new DataTypeFor[Iterable[S]] {
      override def dataType: DataType = ArrayType(elementType.dataType)
    }
  }

  implicit def ListFor[S](implicit elementType: DataTypeFor[S]): DataTypeFor[List[S]] = {
    new DataTypeFor[List[S]] {
      override def dataType: DataType = ArrayType(elementType.dataType)
    }
  }

  implicit def SetFor[S](implicit elementType: DataTypeFor[S]): DataTypeFor[Set[S]] = {
    new DataTypeFor[Set[S]] {
      override def dataType: DataType = ArrayType(elementType.dataType)
    }
  }

  implicit def VectorFor[S](implicit elementType: DataTypeFor[S]): DataTypeFor[Vector[S]] = {
    new DataTypeFor[Vector[S]] {
      override def dataType: DataType = ArrayType(elementType.dataType)
    }
  }

  implicit def SeqFor[S](implicit elementType: DataTypeFor[S]): DataTypeFor[Seq[S]] = new DataTypeFor[Seq[S]] {
    override def dataType: DataType = ArrayType(elementType.dataType)
  }

  implicit def MapFor[V](implicit valueType: DataTypeFor[V]): DataTypeFor[Map[String, V]] = {
    new DataTypeFor[Map[String, V]] {
      override def dataType: DataType = MapType(StringType, valueType.dataType)
    }
  }

  implicit object TimestampFor extends DataTypeFor[Timestamp] {
    override def dataType: DataType = TimestampType
  }

  implicit object LocalDateFor extends DataTypeFor[LocalDate] {
    override def dataType: DataType = DateType
  }

  implicit object LocalDateTimeFor extends DataTypeFor[LocalDateTime] {
    override def dataType: DataType = DateTimeType
  }
}