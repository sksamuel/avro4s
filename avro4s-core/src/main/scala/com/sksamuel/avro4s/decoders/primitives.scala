package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Decoder, DecoderFor}
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util.UUID

object StringDecoder extends Decoder[String] {
  override def decode(value: Any): String = value match {
    case a: String => a.toString
    case a: Array[Byte] => String(a)
    case a: ByteBuffer => String(a.array())
    case a: Utf8 => a.toString
    case a: GenericFixed => String(a.bytes())
  }
}

object IntDecoder extends Decoder[Int] {
  override def decode(value: Any): Int = value match {
    case a: Int => a
    case a: Short => a.toInt
    case a: Byte => a.toInt
  }
}

trait PrimitiveDecoders {

  given Decoder[Int] = IntDecoder
  given Decoder[String] = StringDecoder
  given Decoder[UUID] = StringDecoder.map(UUID.fromString)
//  
  //
  //  given DecoderFor[Long] = DecoderFor {
  //    case a: Long => a
  //    case a: Int => a.toLong
  //    case a: Short => a.toLong
  //    case a: Byte => a.toLong
  //  }
  //
  //  given DecoderFor[Boolean] = DecoderFor {
  //    case a: Boolean => a
  //  }
  //
  //  given DecoderFor[Double] = DecoderFor {
  //    case a: Double => a
  //    case a: Float => a
  //  }
  //
  //  given DecoderFor[Float] = DecoderFor {
  //    case a: Float => a
  //  }
  //
  //  given DecoderFor[ByteBuffer] = DecoderFor {
  //    case a: ByteBuffer => a
  //    case a: Array[Byte] => ByteBuffer.wrap(a)
  //  }
}
