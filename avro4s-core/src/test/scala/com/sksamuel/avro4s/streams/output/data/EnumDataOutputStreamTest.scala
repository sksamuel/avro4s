package com.sksamuel.avro4s.streams.output.data

import org.scalatest.{FunSuite, Matchers}

class EnumDataOutputStreamTest extends FunSuite with Matchers {

  test("java enum") {
    //    val data = WineCrate(Wine.Malbec)
    //
    //    val output = new ByteArrayOutputStream
    //    val avro = AvroOutputStream.data[WineCrate](output)
    //    avro.write(data)
    //    avro.close()
    //
    //    val in = AvroInputStream.data[WineCrate](output.toByteArray)
    //    in.iterator.toList shouldBe List(data)
    //    in.close()
  }
  test("optional java enum") {
    //    val a = JavaEnumOptional(Some(Wine.Malbec))
    //    val b = JavaEnumOptional(None)
    //    val c = JavaEnumOptional(Some(Wine.Merlot))
    //
    //    val output = new ByteArrayOutputStream
    //    val avro = AvroOutputStream.data[JavaEnumOptional](output)
    //    avro.write(List(a, b, c))
    //    avro.close()
    //
    //    val in = AvroInputStream.data[JavaEnumOptional](output.toByteArray)
    //    in.iterator.toList shouldBe List(a, b, c)
    //    in.close()
  }
  test("scala enum") {

  }
  test("optional scala enum") {

  }
}
