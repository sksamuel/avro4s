package com.sksamuel.avro4s.avroutils

import java.nio.ByteBuffer

private[avro4s] object ByteBufferHelper {
  def asArray(byteBuffer: ByteBuffer): Array[Byte] = {
    if (byteBuffer.hasArray) {
      byteBuffer.array()
    } else {
      val bytes = new Array[Byte](byteBuffer.remaining)
      byteBuffer.get(bytes)
      bytes
    }
  }
}
