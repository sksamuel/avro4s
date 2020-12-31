package com.sksamuel.avro4s

import com.sksamuel.avro4s.schemas.{DefinitionEnvironment, SchemaFor}

case class SchemaConfiguration(mapper: FieldMapper, env: DefinitionEnvironment) {
  def withMapper(mapper: FieldMapper): SchemaConfiguration = copy(mapper = mapper)
  def withEnv(env: DefinitionEnvironment): SchemaConfiguration = copy(env = env)
}

object SchemaConfiguration {
  val default = SchemaConfiguration(DefaultFieldMapper, DefinitionEnvironment.empty)
}