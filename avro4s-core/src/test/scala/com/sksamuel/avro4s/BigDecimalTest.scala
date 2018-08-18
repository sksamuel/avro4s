//package com.sksamuel.avro4s
//
//import java.nio.file.Files
//
//import org.scalatest.{FlatSpec, Matchers}
//
//case class BigDecimalDefault(decimal: BigDecimal = 964.55)
//case class BigDecimalSeq(biggies: Seq[BigDecimal])
//case class BigDecimalSeqOption(biggies: Seq[Option[BigDecimal]])
//
//class BigDecimalTest extends FlatSpec with Matchers {
//
//  case class BigDecimalNestedDefault(decimal: BigDecimal = 964.55)
//  case class BigDecimalTest(decimal: BigDecimal)
//  case class BigDecimalOption(decimal: Option[BigDecimal])
//
//  "BigDecimal" should "be serializable" in {
//
//    val file = Files.createTempFile("bigdecimal", ".avro")
//
//    val a = BigDecimalTest(122.75)
//    val b = BigDecimalTest(640.80)
//
//    val out = AvroOutputStream.data[BigDecimalTest](file)
//    out.write(a)
//    out.write(b)
//    out.close()
//
//    val in = AvroInputStream.data[BigDecimalTest](file)
//    in.iterator.toList shouldBe List(a, b)
//    in.close()
//  }
//
//  it should "be serializable" in {
//
//    val file = Files.createTempFile("bigdecimal_option", ".avro")
//
//    val a = BigDecimalOption(Some(123.45))
//    val b = BigDecimalOption(None)
//    val c = BigDecimalOption(Some(641.67))
//
//    val out = AvroOutputStream.data[BigDecimalOption](file)
//    out.write(List(a, b, c))
//    out.close()
//
//    val in = AvroInputStream.data[BigDecimalOption](file)
//    in.iterator.toList shouldBe List(a, b, c)
//    in.close()
//  }
//
//  it should "be serializable" in {
//
//    val file = Files.createTempFile("bigdecimal_default", ".avro")
//
//    val a = BigDecimalDefault(BigDecimal(150.25))
//    val b = BigDecimalDefault()
//    val c = BigDecimalDefault(185.50)
//
//    val out = AvroOutputStream.data[BigDecimalDefault](file)
//    out.write(List(a, b, c))
//    out.close()
//
//    val in = AvroInputStream.data[BigDecimalDefault](file)
//    in.iterator.toList shouldBe List(a, b, c)
//    in.close()
//  }
//
//  it should "be serializable" in {
//
//    val file = Files.createTempFile("bigdecimal_default", ".avro")
//
//    val a = BigDecimalNestedDefault(BigDecimal(150.25))
//    val b = BigDecimalNestedDefault()
//    val c = BigDecimalNestedDefault(185.50)
//
//    val out = AvroOutputStream.data[BigDecimalNestedDefault](file)
//    out.write(List(a, b, c))
//    out.close()
//
//    val in = AvroInputStream.data[BigDecimalNestedDefault](file)
//    in.iterator.toList shouldBe List(a, b, c)
//    in.close()
//  }
//
//
//  it should "be serializable" in {
//
//    val file = Files.createTempFile("bigdecimal_default", ".avro")
//
//    val a = BigDecimalSeq(Seq(150.25, 500.60, 439.60))
//    val b = BigDecimalSeq(Seq(999.72, 852.38, 687.12))
//
//    val out = AvroOutputStream.data[BigDecimalSeq](file)
//    out.write(List(a, b))
//    out.close()
//
//    val in = AvroInputStream.data[BigDecimalSeq](file)
//    in.iterator.toList shouldBe List(a, b)
//    in.close()
//  }
//
//  it should "be serializable" in {
//
//    val file = Files.createTempFile("bigdecimal_default", ".avro")
//
//    val a = BigDecimalSeqOption(Seq(Some(150.25), None, Some(439.60)))
//    val b = BigDecimalSeqOption(Seq(Some(109.44), Some(500.60), None))
//
//    val out = AvroOutputStream.data[BigDecimalSeqOption](file)
//    out.write(List(a, b))
//    out.close()
//
//    val in = AvroInputStream.data[BigDecimalSeqOption](file)
//    in.iterator.toList shouldBe List(a, b)
//    in.close()
//  }
//}
