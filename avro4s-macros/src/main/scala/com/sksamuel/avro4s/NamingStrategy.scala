package com.sksamuel.avro4s

import scala.collection.mutable.ListBuffer

sealed trait NamingStrategy {

  def to(name: String): String = name

  protected def fromDelimited(sep: String, s: String): String = {
    val head :: tail = s.split(sep).toList
    head ++ tail.foldLeft("")((acc, word) => acc ++ word.capitalize)
  }

  protected def toDelimited(delim: Char, s: String): String = {
    val word = ListBuffer.empty[Char]
    word.append(s.head.toLower)
    s.tail.toCharArray.foreach { char =>
      if (char.isUpper) {
        word.append(delim)
      }
      word.append(char.toLower)
    }
    word.result.mkString
  }
}

case object DefaultNamingStrategy extends NamingStrategy {
  override def to(name: String): String = name
}

case object PascalCase extends NamingStrategy {
  override def to(name: String): String = name.head.toUpper + name.tail
}

case object SnakeCase extends NamingStrategy {
  override def to(name: String): String = toDelimited('_', name)
}

case object LispCase extends NamingStrategy {
  override def to(name: String): String = toDelimited('-', name)
}