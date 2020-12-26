package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema

/**
 * Creates an [[Encoder]] when provided with a schema and mapper.
 *
 * Most types don't care about a schema when it comes to encoding. For example, an int
 * is encoded as an int. There's no schema level control over that.
 *
 * But for some types, like BigDecimal, LocalDate, or even String, we can encode these
 * in several ways. For example, a string can be encoded as a string, a fixed-size byte array,
 * or a variable sized byte array. A local date can be encoded as a timestamp with millisecond precision,
 * or as a timestamp with microsecond precision.
 *
 * In addition, for records we need to know if the type is being encoded as an error type, which fields
 * are being included and so on.
 *
 * Therefore in order to correctly handle the encoding and decoding process, the encoders and decoders 
 * need access to the schema that is used.
 *
 * This typeclass exists to create an [[Encoder]] when provided with a schema.
 */