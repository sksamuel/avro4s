package com.sksamuel.avro4s

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import shapeless._
import shapeless.labelled._
import shapeless.ops.record.Keys

import scala.reflect.ClassTag

trait Reader[T] {
  def read(value: Any): T
}

object Reader {

  implicit object HNilReader extends Reader[HNil] {
    override def read(value: Any): HNil = HNil
  }

  implicit object StringReader extends Reader[String] {
    override def read(value: Any): String = value.toString
  }

  implicit object BooleanReader extends Reader[Boolean] {
    override def read(value: Any): Boolean = value.toString.toBoolean
  }

  implicit object FloatReader extends Reader[Float] {
    override def read(value: Any): Float = value.toString.toFloat
  }

  implicit object LongReader extends Reader[Long] {
    override def read(value: Any): Long = value.toString.toLong
  }

  implicit object IntReader extends Reader[Int] {
    override def read(value: Any): Int = value.toString.toInt
  }

  implicit object DoubleReader extends Reader[Double] {
    override def read(value: Any): Double = value.toString.toDouble
  }

  implicit def OptionReader[T](implicit reader: Reader[T]) = new Reader[Option[T]] {
    override def read(value: Any): Option[T] = Option(value).map(reader.read)
  }

  implicit def ArrayReader[T](implicit reader: Reader[T], tag: ClassTag[T]): Reader[Array[T]] = new Reader[Array[T]] {

    import scala.collection.JavaConverters._

    override def read(value: Any): Array[T] = value match {
      case array: Array[T] => array.map(reader.read)
      case list: java.util.Collection[T] => list.asScala.map(reader.read).toArray
    }
  }

  implicit def ListReader[T](implicit reader: Reader[T]): Reader[List[T]] = new Reader[List[T]] {

    import scala.collection.JavaConverters._

    override def read(value: Any): List[T] = value match {
      case array: Array[T] => array.map(reader.read).toList
      case list: java.util.Collection[T] => list.asScala.map(reader.read).toList
    }
  }

  implicit def SeqReader[T](implicit reader: Reader[T]): Reader[Seq[T]] = new Reader[Seq[T]] {

    import scala.collection.JavaConverters._

    override def read(value: Any): Seq[T] = value match {
      case array: Array[T] => array.map(reader.read)
      case list: java.util.Collection[T] => list.asScala.map(reader.read).toList
    }
  }

  implicit def MapReader[T](implicit reader: Reader[T]): Reader[Map[String, T]] = new Reader[Map[String, T]] {

    import scala.collection.JavaConverters._

    override def read(value: Any): Map[String, T] = value match {
      case map: java.util.Map[Any, Any] =>
        map.asScala.toMap.map { case (k, v) => k.toString -> reader.read(v) }
    }
  }

  implicit def GenericReader[T](implicit deser: Lazy[AvroDeserializer[T]]): Reader[T] = new Reader[T] {
    override def read(value: Any): T = value match {
      case record: GenericRecord => deser.value.apply(record)
    }
  }

  import scala.reflect.runtime.universe.WeakTypeTag

  implicit def EitherReader[A, B](implicit
                                  leftReader: Reader[A],
                                  rightReader: Reader[B],
                                  leftType: WeakTypeTag[A],
                                  rightType: WeakTypeTag[B]): Reader[Either[A, B]] = new Reader[Either[A, B]] {
    override def read(value: Any): Either[A, B] = {

      import scala.reflect.runtime.universe.typeOf

      def convert[C](tpe: scala.reflect.runtime.universe.Type): Either[A, B] = {
        if (leftType.tpe <:< tpe) Left(leftReader.read(value))
        else if (rightType.tpe <:< tpe) Right(rightReader.read(value))
        else sys.error(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
      }

      def typeVals(tpe: scala.reflect.runtime.universe.Type): List[String] = {
        tpe.members.filter(_.isTerm).map(_.asTerm).filter(_.isVal).map(_.name.decodedName.toString.trim).toList
      }

      // if we have a generic record, we can't use the type to work out which one it matches,
      // so we have to compare field names
      def fromRecord(record: GenericRecord): Either[A, B] = {
        import scala.collection.JavaConverters._
        val fieldNames = record.getSchema.getFields.asScala.map(_.name).toList
        if (typeVals(leftType.tpe).toSet == fieldNames.toSet) Left(leftReader.read(value))
        else if (typeVals(rightType.tpe).toSet == fieldNames.toSet) Right(rightReader.read(value))
        else sys.error(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
      }

      value match {
        case utf8: Utf8 => convert(typeOf[String])
        case true | false => convert(typeOf[Boolean])
        case _: Int => convert(typeOf[Int])
        case _: Long => convert(typeOf[Long])
        case _: Double => convert(typeOf[Double])
        case _: Float => convert(typeOf[Float])
        case record: GenericData.Record => fromRecord(record)
        case _ => sys.error(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
      }
    }
  }
}

trait AvroDeserializer[T] {
  def apply(record: GenericRecord): T
}

object AvroDeserializer {

  implicit object HNilDeserializer extends AvroDeserializer[HNil] {
    def apply(record: GenericRecord): HNil = HNil
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       reader: Lazy[Reader[V]],
                                                       remaining: AvroDeserializer[T]): AvroDeserializer[FieldType[K, V] :: T] = {
    new AvroDeserializer[FieldType[K, V] :: T] {

      val fieldName = key.value.name

      override def apply(record: GenericRecord): FieldType[K, V] :: T = {
        val v = reader.value.read(record.get(key.value.name))
        field[K](v) :: remaining(record)
      }
    }
  }

  implicit def GenericSer[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                            deser: Lazy[AvroDeserializer[Repr]]) = new AvroDeserializer[T] {
    override def apply(record: GenericRecord): T = labl.from(deser.value.apply(record))
  }
}