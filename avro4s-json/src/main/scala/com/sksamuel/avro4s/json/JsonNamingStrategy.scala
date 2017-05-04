package com.sksamuel.avro4s.json

/**
  * User: netanelrabinowitz
  * Date: 02/05/2017
  * Time: 20:57
  */
sealed trait JsonNamingStrategy
case object CamelCase extends JsonNamingStrategy
case object PascalCase extends JsonNamingStrategy
case object SnakeCase extends JsonNamingStrategy
case object LispCase extends JsonNamingStrategy
