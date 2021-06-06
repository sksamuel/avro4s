//package com.sksamuel.avro4s.streams.input
//
//import java.util.UUID
//
//class EitherInputStreamTest extends InputStreamTest {
//
//  test("read either of uuids") {
//    case class Test(z: Either[Double, UUID])
//    writeRead(Test(Right(UUID.randomUUID())))
//  }
//}