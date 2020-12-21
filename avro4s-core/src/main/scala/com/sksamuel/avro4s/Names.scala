package com.sksamuel.avro4s

import scala.quoted._

class Names(quotes: Quotes)(classdef: quotes.reflect.ClassDef, symbol: quotes.reflect.Symbol) {

  import quotes.reflect._

  // this needs to be improved to use matching on parents rather than name mangling hacks
  private val defaultNamespace: String = symbol.owner.fullName.replaceAll("\\.<local .*?>", "").stripSuffix(".package")

  /**
   * Returns the default namespace for this class, which is the package name.
   */
  def namespace: String = defaultNamespace
}
