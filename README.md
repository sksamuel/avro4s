# avro4s

[![Join the chat at https://gitter.im/sksamuel/avro4s](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/sksamuel/avro4s?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Build Status](https://travis-ci.org/sksamuel/avro4s.png)](https://travis-ci.org/sksamuel/avro4s)

Avro4s is a schema generation and serializing/deserializing library for Avro written in Scala. The objective is to
allow seemless use with Scala without the need to to write boilerplate conversions yourself, and without the runtime overhead of reflection. Hence, this is a macro based
library and generates code for use with avro at _compile time_.

## Serializing

Avro4s allows us to easily serialize Scala case classes into an avro stream. 

Lets first create some instances to serializer:

```scala
case class Artist(name: String, yearOfBirth: Int, yearOfDeath: Int, birthplace: String, methods: Seq[String])
val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
val raphael = Artist("raphael", 1483, 1520, "Florence", Seq("painter", "architect"))
```

Then we can create an `AvroOutputStream` which we write to, and close, just like you would any regular output stream.
Note that when we create the output stream we must specify the type it will accept.

```scala
import AvroImplicits._
import java.nio.file.Paths
import com.sksamuel.avro4s.AvroOutputStream 

val path = Paths.get("artists.avro")
val out = AvroOutputStream[Artist](path)
out.write(painters)
out.close()
```

## Deserializing

With avro4s we can easily deserialize a file back into Scala case classes. Given the `artists.avro` file we generated
in the previous section on serialization, we will read this back in using the `AvroInputStream` class.

```scala
implicit val s = AvroImplicits.schemaFor[Artist]
implicit val r = AvroImplicits.populatorFor[Artist]

val path = Paths.get("artists.avro")
val in = AvroInputStream[Artist](path)
val painters = in.iterator.toSet
println(painters) // 
in.close()
```

## Schemas

To generate a schema for a given case class, we need to import a schema implicit and that's all.

```scala
implicit val s = AvroImplicits.schemaFor[Artist]
println(s.schema)
```

would output

```json
{  
   "type":"record",
   "name":"Artist",
   "namespace":"com.sksamuel.avro4s",
   "fields":[  
      {  
         "name":"name",
         "type":"string"
      },
      {  
         "name":"yearOfBirth",
         "type":"int"
      },
      {  
         "name":"yearOfDeath",
         "type":"int"
      },
      {  
         "name":"birthplace",
         "type":"string"
      },
      {  
         "name":"styles",
         "type":{  
            "type":"array",
            "items":"string"
         }
      }
   ]
}
```


## Type Mappings

### Primitives

|Scala Type|Avro Type|
|----------|---------|
|Boolean|boolean|
|String|string|
|Int|int|
|Long|long|
|BigDecimal|decimal|
|Double|double|
|Float|float|

### Complex Types

|Scala Type|Avro Type|
|----------|---------|
|scala.collection.Array[T]|array|
|scala.collection.List[T]|array|
|scala.collection.Seq[T]|array|
|scala.collection.Iterable[T]|array|
|scala.collection.Set[T]|array|
|scala.collection.Map[String, T]|map|
|Double|double|
|Float|float|
|T|record|

## Todo

Features to be added before 1.0 release

* Annotation for Avro properties 
* Annotation for aliases
* Union and fixed types
* Error handling during deserialization

## Using Elastic4s in your project

Gradle: `compile 'com.sksamuel.avro4s:avro4s-core_2.11:0.90.0'`

SBT: `libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "0.90.0"`

Maven:

```xml
<dependency>
    <groupId>com.sksamuel.avro4s</groupId>
    <artifactId>avro4s-core_2.11</artifactId>
    <version>0.90.0</version>
</dependency>
```

The above is just an example and is not always up to date. Check the latest released version on
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
