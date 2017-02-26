package com.sksamuel.avro4s.examples.UppercasePkg

import shapeless.{:+:, CNil}

case class Data(payload: String :+: Int :+: CNil)
