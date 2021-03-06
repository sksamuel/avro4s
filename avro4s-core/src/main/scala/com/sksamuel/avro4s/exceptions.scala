package com.sksamuel.avro4s

class Avro4sException(message: String) extends Exception(message: String)

class Avro4sConfigurationException(message: String) extends Avro4sException(message)

class Avro4sEncodingException(message: String) extends Avro4sException(message)

class Avro4sDecodingException(message: String, val value: Any) extends Avro4sException(message)
