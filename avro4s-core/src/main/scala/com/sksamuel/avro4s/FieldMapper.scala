package com.sksamuel.avro4s

/**
 * We may have a schema with a field in snake case like say { "first_name": "sam" } and
 * that schema needs to be used for a case class field `firstName`.
 *
 * The [[FieldMapper]] is used to map fields in a schema to fields in a class by
 * transforming the class field name to the wire name format.
 */
trait FieldMapper {

  /**
   * Takes a field name and converts it to the wire format.
   * Eg, invoking this method with "wibbleWobble" on [[SnakeCase]] would return `wibble_wobble`.
   */
  def to(name: String): String = name

  protected def fromDelimited(sep: String, s: String): String = {
    val head :: tail = s.split(sep).toList
    head ++ tail.foldLeft("")((acc, word) => acc ++ word.capitalize)
  }

  protected def toDelimited(delim: Char, s: String): String = {
    val word = scala.collection.mutable.ListBuffer.empty[Char]
    word += s.head.toLower
    s.tail.toCharArray.foreach { char =>
      if (char.isUpper) {
        word.+=(delim)
      }
      word.+=(char.toLower)
    }
    word.result.mkString
  }
}

/**
 * The default implementation of [[FieldMapper]] that returns the input without modification.
 */
case object DefaultFieldMapper extends FieldMapper {
  override def to(name: String): String = name
}

/**
 * Converts names to their PascalCaseVersion.
 */
case object PascalCase extends FieldMapper {
  override def to(name: String): String = {
    if (name.length == 1) name.toUpperCase else {
      val chars = name.toCharArray
      chars(0) = chars(0).toUpper
      new String(chars)
    }
  }
}

/**
 * Converts names to their snake_case_version.
 */
case object SnakeCase extends FieldMapper {
  override def to(name: String): String = toDelimited('_', name)
}