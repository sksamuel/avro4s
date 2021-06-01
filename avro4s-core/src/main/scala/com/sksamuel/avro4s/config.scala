package com.sksamuel.avro4s

case class SchemaConfiguration(mapper: FieldMapper) {
  def withMapper(mapper: FieldMapper): SchemaConfiguration = copy(mapper = mapper)
//  def withEnv(env: DefinitionEnvironment): SchemaConfiguration = copy(env = env)
}

object SchemaConfiguration {
  val default = SchemaConfiguration(DefaultFieldMapper)
}