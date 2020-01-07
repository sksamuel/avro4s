package com.sksamuel.avro4s;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Adds an addition property to a java type when avro4s converts the type to an AVRO schema.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface AvroJavaProp {
    String key();
    String value();
}
