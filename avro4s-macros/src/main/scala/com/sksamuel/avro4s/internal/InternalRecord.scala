package com.sksamuel.avro4s.internal

import org.apache.avro.Schema

case class InternalRecord(schema: Schema, values: Vector[Any])
