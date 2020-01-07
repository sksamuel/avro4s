package com.sksamuel.avro4s.schema;

import com.sksamuel.avro4s.AvroJavaEnumDefault;
import com.sksamuel.avro4s.AvroJavaName;
import com.sksamuel.avro4s.AvroJavaNamespace;
import com.sksamuel.avro4s.AvroJavaProp;

@AvroJavaName("Wine")
@AvroJavaNamespace("test")
@AvroJavaProp(key = "hello", value = "world")
public enum WineWithAnnotations {
    Malbec,
    Shiraz,
    @AvroJavaEnumDefault CabSav,
    Merlot
}
