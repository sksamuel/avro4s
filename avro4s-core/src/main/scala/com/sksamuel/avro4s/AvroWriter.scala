//package com.sksamuel.avro4s
//
//import java.nio.ByteBuffer
//
//import org.apache.avro.generic.GenericRecord
//import shapeless._
//import shapeless.labelled.FieldType
//import scala.collection.JavaConverters._
//
//import scala.language.implicitConversions
//import scala.reflect.ClassTag
//
//trait ToValue[-A] {
//  def apply(value: A): Any = value
//}
//
//object ToValue {
//
//  implicit object BooleanToValue extends ToValue[Boolean]
//
//  implicit object StringToValue extends ToValue[String]
//
//  implicit object DoubleToValue extends ToValue[Double]
//
//  implicit object FloatToValue extends ToValue[Float]
//
//  implicit object IntToValue extends ToValue[Int]
//
//  implicit object LongToValue extends ToValue[Long]
//
//  implicit object BigDecimalToValue extends ToValue[BigDecimal] {
//    override def apply(value: BigDecimal): ByteBuffer = ByteBuffer.wrap(value.toString.getBytes)
//  }
//
//  implicit def EitherWriter[T, U](implicit lefttovalue: ToValue[T], righttovalue: ToValue[U]) = new ToValue[Either[T, U]] {
//    override def apply(value: Either[T, U]): Any = value match {
//      case Left(left) => lefttovalue(left)
//      case Right(right) => righttovalue(right)
//    }
//  }
//
//  implicit def OptionWriter[T](implicit tovalue: ToValue[T]) = new ToValue[Option[T]] {
//    override def apply(value: Option[T]): Any = value.map(tovalue.apply).orNull
//  }
//
//  implicit def ArrayWriter[T](implicit tovalue: Lazy[ToValue[T]]): ToValue[Array[T]] = new ToValue[Array[T]] {
//    override def apply(value: Array[T]): Any = value.headOption match {
//      case Some(b: Byte) => ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
//      case _ => value.map(tovalue.value.apply).toSeq.asJavaCollection
//    }
//  }
//
//  implicit object ByteArrayToValue extends ToValue[Array[Byte]] {
//    override def apply(value: Array[Byte]): ByteBuffer = ByteBuffer.wrap(value)
//  }
//
//  implicit def SetWriter[T](implicit tovalue: Lazy[ToValue[T]]): ToValue[Set[T]] = new ToValue[Set[T]] {
//    override def apply(values: Set[T]): java.util.Collection[Any] = values.map(tovalue.value.apply).asJavaCollection
//  }
//
//  implicit def SeqWriter[T](implicit tovalue: Lazy[ToValue[T]]): ToValue[Seq[T]] = new ToValue[Seq[T]] {
//    override def apply(values: Seq[T]): Any = values.map(tovalue.value.apply).asJava
//  }
//
//  implicit def MapWriter[T](implicit tovalue: Lazy[ToValue[T]]) = new ToValue[Map[String, T]] {
//    override def apply(value: Map[String, T]): java.util.Map[String, T] = {
//      value.mapValues(tovalue.value.apply).asInstanceOf[Map[String, T]].asJava
//    }
//  }
//
//  implicit def EnumWriter[E <: Enum[E]]: ToValue[Enum[E]] = new ToValue[Enum[E]] {
//    override def apply(value: Enum[E]): Any = value.name
//  }
//
//  implicit def GenericWriter[T](implicit writer: Lazy[AvroWriter[T]]): ToValue[T] = new ToValue[T] {
//    override def apply(value: T): GenericRecord = writer.value(value)
//  }
//}
//
//trait AvroMapper[T] {
//  def apply(t: T): Map[String, Any]
//}
//
//object AvroMapper {
//
//  implicit object HNilFields extends AvroMapper[HNil] {
//    override def apply(value: HNil): Map[String, Any] = Map.empty
//  }
//
//  implicit def HConsFields[Key <: Symbol, V, T <: HList](implicit key: Witness.Aux[Key],
//                                                         tovalue: Lazy[ToValue[V]],
//                                                         remaining: AvroMapper[T],
//                                                         tag: ClassTag[V]): AvroMapper[FieldType[Key, V] :: T] = {
//    new AvroMapper[FieldType[Key, V] :: T] {
//      override def apply(value: FieldType[Key, V] :: T): Map[String, Any] = value match {
//        case h :: t => remaining(t) + ((key.value.name, tovalue.value(h)))
//      }
//    }
//  }
//}
//
//trait AvroWriter[T] {
//  def apply(t: T): GenericRecord
//}
//
//object AvroWriter {
//
//  implicit def GenericWriter[T, Repr <: HList](implicit
//                                               gen: LabelledGeneric.Aux[T, Repr],
//                                               mapper: AvroMapper[Repr],
//                                               schema: Lazy[ToSchema[T]]) = new AvroWriter[T] {
//    override def apply(t: T): GenericRecord = {
//      val map = mapper(gen.to(t))
//      val r = new org.apache.avro.generic.GenericData.Record(schema.value.apply)
//      map.foreach { case (k, v) => r.put(k, v) }
//      r
//    }
//  }
//
//  def apply[T](implicit writer: Lazy[AvroWriter[T]]): AvroWriter[T] = writer.value
//}