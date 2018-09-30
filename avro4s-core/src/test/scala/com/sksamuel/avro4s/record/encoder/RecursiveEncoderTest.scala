package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.schema.Recursive
import org.scalatest.{FunSuite, Matchers}

class RecursiveEncoderTest extends FunSuite with Matchers {

  ignore("recursive encoding") {
    val data = Recursive(4, Some(Recursive(2, Some(Recursive(9, None)))))

    // at the moment you need to store the recursive schema in a variable
    // so that the compiler doesn't try to expand it forever
    // implicit val schema = SchemaFor[Recursive]
  }
}


