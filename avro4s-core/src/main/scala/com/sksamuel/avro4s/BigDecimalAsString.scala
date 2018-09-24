package com.sksamuel.avro4s

import com.sksamuel.avro4s.internal.{DataType, DataTypeFor, FixedType, StringType}

object BigDecimalAsString {
  implicit object BigDecimalAsStringCodec extends DataTypeFor[BigDecimal] {
    override def dataType: DataType = StringType
  }
}

object BigDecimalAsFixed {
  implicit object BigDecimalAsFixedCodec extends DataTypeFor[BigDecimal] {
    override def dataType: DataType = FixedType("myfixedtype", 12)
  }
}
