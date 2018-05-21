package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.scalatest.FunSuite

class GenericClassTest extends FunSuite {
  test("Generic classes with different concrete types generate different type names") {
    val bos = new ByteArrayOutputStream()
    AvroOutputStream.data[MyData](bos).write(MyData(MyWrapper(1), Some(MyWrapper(""))))
    assert(AvroOutputStream.data[MyData](bos).schema.toString() ===
      """{"type":"record","name":"MyData","namespace":"com.sksamuel.avro4s","fields":[{"name":"i","type":{"type":"record","name":"MyWrapper_Int","fields":[{"name":"a","type":"int"}]}},{"name":"s","type":["null",{"type":"record","name":"MyWrapper_String","fields":[{"name":"a","type":"string"}]}]}]}""")
  }
}
