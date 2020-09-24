package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8

enum AvroValue {

  case AvroString(str: String)
  case AvroUtf8(ut8: Utf8)
  case AvroByteArray(bytes: Array[Byte])
  case AvroByteBuffer(buffer: ByteBuffer)
  case AvroBoolean(boolean: Boolean)

  case AvroByte(byte: Byte)
  case AvroShort(short: Short)
  case AvroInt(int: Int)
  case AvroLong(long: Long)
  case AvroFloat(float: Float)
  case AvroDouble(double: Double)

  case AvroEnumSymbol(symbol: GenericEnumSymbol[_])
  case AvroList(list: List[AvroValue])
  case AvroMap(map: Map[String, AvroValue])
  case AvroNull
  case Fixed(fixed: GenericFixed)

  case AvroRecord(record: GenericRecord)
}

extension(r: AvroValue.AvroRecord) def get(name: String): Option[AvroValue] = 
  Option(r.record.get(name)).map(unsafeFromAny)
  
extension(r: AvroValue.AvroRecord) def get(i: Int): Option[AvroValue] = 
  Option(r.record.get(i)).map(unsafeFromAny)

def unsafeFromAny(a: Any): AvroValue = if (a == null) AvroValue.AvroNull else a match {
  case r: GenericRecord => AvroValue.AvroRecord(r)
  case i: java.lang.Integer => AvroValue.AvroInt(i)
  case l: java.lang.Long => AvroValue.AvroLong(l)
  case b: java.lang.Boolean => AvroValue.AvroBoolean(b)
  case f: java.lang.Float => AvroValue.AvroFloat(f)
  case d: java.lang.Double => AvroValue.AvroDouble(d)
  case b: java.lang.Byte => AvroValue.AvroByte(b)
  case s: java.lang.Short => AvroValue.AvroShort(s)
  case c: CharSequence => AvroValue.AvroString(c.toString)
  case a: Array[Byte] => AvroValue.AvroByteArray(a)
  case a: Array[_] => AvroValue.AvroList(a.toList.map(unsafeFromAny))
  case e: GenericEnumSymbol[_] => AvroValue.AvroEnumSymbol(e)
  case g: GenericFixed => AvroValue.Fixed(g)
  case b: ByteBuffer => AvroValue.AvroByteArray(b.array())
  //  case m: java.util.Map[_, _] => AvroMap (m.asScala.map {
  //  case (key, value) => key.toString -> unsafeFromAny (value)
  //}.toMap)
  //  case c: java.lang.Iterable[_] => AvroList (c.asScala.map (AvroValue.unsafeFromAny).toList)
  case _ => throw new Avro4sUnsupportedValueException(s"$a is not a supported type when decoding. All types must be wrapped in AvroValue.")
  //}
}

class Avro4sUnsupportedValueException(msg: String) extends RuntimeException(msg)
class Avro4sEncodingException(msg: String) extends RuntimeException(msg)
class Avro4sConfigurationException(msg: String) extends RuntimeException(msg)