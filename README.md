# avro4s

[![Join the chat at https://gitter.im/sksamuel/avro4s](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/sksamuel/avro4s?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Build Status](https://travis-ci.org/sksamuel/avro4s.png)](https://travis-ci.org/sksamuel/avro4s)

Avro4s is a scheme/class generation and serializing/deserializing library for [Avro](http://avro.apache.org/) written in Scala. The objective is to allow seamless use with Scala without the need to to write boilerplate conversions yourself, and without the runtime overhead of reflection. Hence, this is a macro based library and generates code for use with avro at _compile time_.

The features of the library are: 
* Schema generation from classes at compile time
* Class generation from schemas at build time
* Boilerplate free serialization of classes to avro
* Boilerplate free deserialization of avro to classes

## Changelog
* 1.3.0 - Added support for Scala 2.12. Removed 2.10 cross build. Fixed issues with private vals. Added binary (no schema) output stream. Exposed RecordFormat[T] typeclass to enable easy conversion of T to/from an avro Record.
* 1.2.0 - Added support for properties, doc fields, and aliases. These are set via annotations.
* 1.1.0 - Added json document to avro schema converter
* 1.0.0 - Migrated all macros to use Shapeless. Fixed some trickier nested case class issues. Simplified API. Added support for java enums.
* 0.94.0 - Added support for writing/reading Either and Option in serializer/deserializer. Fixed bug with array serialization.
* 0.93.0 - Added support for either and options in schema generator. Added support for aliases via scala annotation.
* 0.92.0 - Added support for unions (and unions of nulls to Options) and enums to class generator.

## Schemas

Avro4s allows us to generate schemas directly from classes in a totally straightforward way. Let's define some classes.

```scala
case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
case class Ingredient(name: String, sugar: Double, fat: Double)
```

Next is to invoke the apply method of AvroSchema passing in the top level type. This will return an `org.apache.avro.Schema` instance, from which you can output, write to a file etc.

```scala
import com.sksamuel.avro4s.AvroSchema
val schema = AvroSchema[Pizza]
```

Which will output the following schema:

```json
{
  "type" : "record",
  "name" : "Pizza",
  "namespace" : "com.sksamuel.avro4s.json",
  "fields" : [ {
    "name" : "name",
    "type" : "string"
  }, {
    "name" : "ingredients",
    "type" : {
      "type" : "array",
      "items" : {
        "type" : "record",
        "name" : "Ingredient",
        "fields" : [ {
          "name" : "name",
          "type" : "string"
        }, {
          "name" : "sugar",
          "type" : "double"
        }, {
          "name" : "fat",
          "type" : "double"
        } ]
      }
    }
  }, {
    "name" : "vegetarian",
    "type" : "boolean"
  }, {
    "name" : "vegan",
    "type" : "boolean"
  }, {
    "name" : "calories",
    "type" : "int"
  } ]
}
```
You can see that the schema generator handles nested case classes, sequences, primitives, etc. For a full list of supported object types, see the table later.

## Serializing

Avro4s allows us to easily serialize case classes using an instance of `AvroOutputStream` which we write to, and close, just like you would any regular output stream. An `AvroOutputStream` can be created from a `File`, `Path`, or by wrapping another `OutputStream`. When we create one, we specify the type of objects that we will be serializing. Eg, to serialize instances of our Pizza class:

```scala
import java.io.File
import com.sksamuel.avro4s.AvroOutputStream

val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, false, 91)

val os = AvroOutputStream[Pizza](new File("pizzas.avro"))
os.write(Seq(pepperoni, hawaiian))
os.flush()
os.close()
```

## Deserializing

With avro4s we can easily deserialize a file back into Scala case classes. Given the pizzas.avro file we generated in the previous section on serialization, we will read this back in using the `AvroInputStream` class. We first create an instance of the input stream specifying the types we will read back, and the file. Then we call iterator which will return a lazy iterator (reads on demand) of the data in the file. In this example, we'll load all data at once from the iterator via `toSet`.

```scala
import com.sksamuel.avro4s.AvroInputStream

val is = AvroInputStream[Pizza](new File("pizzas.avro"))
val pizzas = is.iterator.toSet
is.close()
println(pizzas.mkString("\n"))
```

Will print out:

```scala
Pizza(pepperoni,List(Ingredient(pepperoni,12.2,4.4), Ingredient(onions,1.2,0.4)),false,false,500) Pizza(hawaiian,List(Ingredient(ham,1.5,5.6), Ingredient(pineapple,5.2,0.2)),false,false,500)
```

## Conversions to/from GenericRecord

To interface with the Java api it is sometimes desirable to convert between your classes and the avro GenericRecord type. You can do this easily in Avro4s using the RecordFormat typeclass (this is what the input/output streams use behind the scenes). Eg,

To convert from a class into a record:

```scala
case class Composer(name: String, birthplace: String, compositions: Seq[String])
val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
val format = RecordFormat[Composer]
// record is of type GenericRecord
val record = format.to(ennio)
```

And to go from a record back into a type:

```
// given some record from earlier
val record = ...
val format = RecordFormat[Composer]
// is an instance of Composer
val ennio = format.from(record)
```

## Type Mappings

|Scala Type|Avro Type|
|----------|---------|
|Boolean|boolean|
|Array[Byte]|bytes|
|String|string or fixed|
|Int|int|
|Long|long|
|BigDecimal|[decimal](https://avro.apache.org/docs/1.7.7/spec.html#Decimal) with scale 2 and precision 8|
|Double|double|
|Float|float|
|sealed trait T|enum|
|scala.collection.Array[T]|array|
|scala.collection.List[T]|array|
|scala.collection.Seq[T]|array|
|scala.collection.Iterable[T]|array|
|scala.collection.Set[T]|array|
|scala.collection.Map[String, T]|map|
|scala.collection.Option[T]|union:null,T|
|scala.collection.Either[L, R]|union:L,R|
|T|record|

## Using avro4s in your project

Gradle: `compile 'com.sksamuel.avro4s:avro4s-core_2.11:1.3.0'`

SBT: `libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.3.0"`

Maven:

```xml
<dependency>
    <groupId>com.sksamuel.avro4s</groupId>
    <artifactId>avro4s-core_2.11</artifactId>
    <version>1.3.0</version>
</dependency>
```

The above is just an example and is not always up to date with the latest version. Check the latest released version on
[maven central](http://search.maven.org/#search|ga|1|g%3A%22com.sksamuel.avro4s%22)

## Building and Testing

This project is built with SBT. So to build
```
sbt compile
```

And to test
```
sbt test
```

## Contributions
Contributions to avro4s are always welcome. Good ways to contribute include:

* Raising bugs and feature requests
* Fixing bugs and enhancing the DSL
* Improving the performance of avro4s
* Adding to the documentation

## License
```
The MIT License (MIT)

Copyright (c) 2015 Stephen Samuel

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

```
