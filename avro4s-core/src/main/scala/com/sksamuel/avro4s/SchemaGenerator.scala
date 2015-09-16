package com.sksamuel.avro4s

import org.apache.avro.{SchemaBuilder, Schema}

object SchemaGenerator {

  def generate(): Schema = {
    SchemaBuilder
      .record("HandshakeRequest").namespace("org.apache.avro.ipc")
      .fields()
      .name("clientHash").`type`().fixed("MD5").size(16).noDefault()
      .name("clientProtocol").`type`().nullable().stringType().noDefault()
      .name("meta").`type`().nullable().map().values().bytesType().noDefault()
      .endRecord()
  }
}
