package com.sksamuel.avro4s

import java.io.File
import java.nio.file.Path

class ClassGenerator {


}

object ClassGenerator {
  def load(path: Path): ClassGenerator = new ClassGenerator
  def load(file: File): ClassGenerator = new ClassGenerator
}
