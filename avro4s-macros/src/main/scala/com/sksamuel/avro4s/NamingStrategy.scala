package com.sksamuel.avro4s

sealed trait NamingStrategy
case object CamelCase extends NamingStrategy
case object PascalCase extends NamingStrategy
case object SnakeCase extends NamingStrategy
case object LispCase extends NamingStrategy
