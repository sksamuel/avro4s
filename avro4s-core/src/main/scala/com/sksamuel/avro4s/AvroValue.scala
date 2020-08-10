package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8

sealed trait AvroValue

object AvroValue {

  import scala.collection.JavaConverters._

  case class AvroString(s: String) extends AvroValue
  case class AvroByteArray(bytes: Array[Byte]) extends AvroValue
  case class AvroGenericFixed(fixed: GenericFixed) extends AvroValue
  case class AvroBoolean(boolean: Boolean) extends AvroValue
  case class AvroShort(short: Short) extends AvroValue
  case class AvroByte(byte: Byte) extends AvroValue
  case class AvroDouble(double: Double) extends AvroValue
  case class AvroFloat(float: Float) extends AvroValue
  case class AvroInt(int: Int) extends AvroValue
  case class AvroLong(long: Long) extends AvroValue
  case class AvroEnumSymbol(symbol: GenericEnumSymbol[_]) extends AvroValue
  case class AvroList(list: List[AvroValue]) extends AvroValue
  case class AvroMap(map: Map[String, AvroValue]) extends AvroValue
  case object AvroNull extends AvroValue

  case class AvroRecord(record: GenericRecord) extends AvroValue {
    def get(name: String): Option[AvroValue] = Option(record.get(name)).map(AvroValue.unsafeFromAny)
    def get(i: Int): Option[AvroValue] = Option(record.get(i)).map(AvroValue.unsafeFromAny)
  }

  private[avro4s] def unsafeFromAny(a: Any): AvroValue = a match {
    case r: GenericRecord => AvroRecord(r)
    case s: String => AvroString(s)
    case u: Utf8 => AvroString(u.toString)
    case c: CharSequence => AvroString(c.toString)
    case b: Boolean => AvroBoolean(b)
    case g: GenericFixed => AvroGenericFixed(g)
    case b: ByteBuffer => AvroByteArray(b.array())
    case a: Array[Byte] => AvroByteArray(a)
    case a: Array[_] => AvroList(a.toList.map(AvroValue.unsafeFromAny))
    case m: java.util.Map[_, _] => AvroMap(m.asScala.map { case (key, value) => key.toString -> unsafeFromAny(value) }.toMap)
    case c: java.lang.Iterable[_] => AvroList(c.asScala.map(AvroValue.unsafeFromAny).toList)
    case c: java.util.Collection[_] => AvroList(c.asScala.map(AvroValue.unsafeFromAny).toList)
    case f: Float => AvroFloat(f)
    case f: java.lang.Float => AvroFloat(f)
    case d: Double => AvroDouble(d)
    case d: java.lang.Double => AvroDouble(d)
    case b: Byte => AvroByte(b)
    case b: java.lang.Byte => AvroByte(b)
    case s: Short => AvroShort(s)
    case s: java.lang.Short => AvroShort(s)
    case i: Int => AvroInt(i)
    case i: java.lang.Integer => AvroInt(i)
    case l: Long => AvroLong(l)
    case l: java.lang.Long => AvroLong(l)
    case e: GenericEnumSymbol[_] => AvroEnumSymbol(e)
    case null => AvroNull
    case _ => throw new Avro4sUnsupportedValueException(s"$a is not a supported type when decoding. All types must be wrapped in AvroValue.")
  }
}
