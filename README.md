# avro4s

[![Build Status](https://travis-ci.org/sksamuel/avro4s.png)](https://travis-ci.org/sksamuel/avro4s)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avro4s/avro4s-core_2.11.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22avro4s-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avro4s/avro4s-core_2.12.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Cavro4s-core_2.12)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avro4s/avro4s-core_2.13.0-M5.svg?label=latest%20release%20for%202.13"/>](http://search.maven.org/#search%7Cga%7C1%7Cavro4s-core_2.13)

Avro4s is a schema/class generation and serializing/deserializing library for [Avro](http://avro.apache.org/) written in Scala. The objective is to allow seamless use with Scala without the need to to write boilerplate conversions yourself, and without the runtime overhead of reflection. Hence, this is a macro based library and generates code for use with Avro at _compile time_.

The features of the library are:
* Schema generation from classes at compile time
* Boilerplate free serialization of Scala types into Avro types
* Boilerplate free deserialization of Avro types to Scala types

## Schemas

Unlike Json, Avro is a schema based format. You'll find yourself wanting to generate schemas frequently, and writing these by hand or through the Java based `SchemaBuilder`
classes can be tiring for complex domain models. Avro4s allows us to generate schemas directly from case classes at compile time via macros. This gives you both the convenience
of generated code, without the annoyance of having to run a code generation step, as well as avoiding the peformance penalty of runtime generated code.

Let's define some classes.

```scala
case class Ingredient(name: String, sugar: Double, fat: Double)
case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
```

To generate an Avro Schema, we need to invoke the apply method of the `AvroSchema` object passing in the target type as a type parameter.
This will return an `org.apache.avro.Schema` instance.

```scala
import com.sksamuel.avro4s.AvroSchema
val schema = AvroSchema[Pizza]
```

Where the generated schema is as follows:

```json
{
   "type":"record",
   "name":"Pizza",
   "namespace":"com.sksamuel.avro4s.json",
   "fields":[
      {
         "name":"name",
         "type":"string"
      },
      {
         "name":"ingredients",
         "type":{
            "type":"array",
            "items":{
               "type":"record",
               "name":"Ingredient",
               "fields":[
                  {
                     "name":"name",
                     "type":"string"
                  },
                  {
                     "name":"sugar",
                     "type":"double"
                  },
                  {
                     "name":"fat",
                     "type":"double"
                  }
               ]
            }
         }
      },
      {
         "name":"vegetarian",
         "type":"boolean"
      },
      {
         "name":"vegan",
         "type":"boolean"
      },
      {
         "name":"calories",
         "type":"int"
      }
   ]
}
```
You can see that the schema generator handles nested case classes, sequences, primitives, etc. For a full list of supported object types, see the table later.

### Overriding class name and namespace

Avro schemas for complex types (RECORDS) contain a name and a namespace. By default, these are the name of the class
and the enclosing package name, but it is possible to customize these using the annotations `AvroName` and `AvroNamespace`.

For example, the following class:

```scala
package com.foo
case class Test(a: String)
```

Would normally have a schema like this:

```json
{
  "type":"record",
  "name":"Test",
  "namespace":"com.foo",
  "fields":[
    {
      "name":"a",
      "type":"string"
    }
  ]
}
```

However we can override the name and/or the namespace like this:

```scala
package com.foo

@AvroName("Wibble")
@AvroNamespace("com.other")
case class Test(a: String)
```

And then the generated schema looks like this:

```json
{
  "type":"record",
  "name":"Wibble",
  "namespace":"com.other",
  "fields":[
    {
      "name":"a",
      "type":"string"
    }
  ]
}
```

Note: It is possible, but not necessary, to use both AvroName and AvroNamespace. You can just use one of them if you wish.


### Overriding a field name

The `AvroName` annotation can also be used to override field names. This is useful when the record instances you are generating or reading
need to have field names different from the scala case classes.

For example, given the following class.

```scala
case class Foo(a: String, @AvroName("z") b : String)
```

Then a record of `Foo("a", "b")` would be written as `{ a: "a", z: "b" }`. Similarly, this record could be decoded back
to an instance of Foo.

Note: @AvroName does not add an alternative name for the field, but an override. If you wish to have alternatives then you want to use @AvroAlias.

### Adding properties and docs to a Schema

Avro allows a doc field, and arbitrary key/values to be added to generated schemas. Avro4s supports this through the use of `AvroDoc` and `AvroProp` annotations.

These properties works on either complex or simple types - in other words, on both fields and classes. For example:

```scala
package com.sksamuel
@AvroDoc("hello its me")
case class Example(@AvroDoc("I am a string") str: String, @AvroDoc("I am a long") long: Long, int: Int)
```

Would result in the following schema:

```json
{  
  "type": "record",
  "name": "Example",
  "namespace": "com.sksamuel",
  "doc":"hello its me",
  "fields": [  
    {  
      "name": "str",
      "type": "string",
      "doc" : "I am a string"
    },
    {  
      "name": "long",
      "type": "long",
      "doc" : "I am a long"
    },
    {  
      "name": "int",
      "type": "int"
    }
  ]
}
```

An example of properties:

```scala
package com.sksamuel
@AvroProp("jack", "bruce")
case class Annotated(@AvroProp("richard", "ashcroft") str: String, @AvroProp("kate", "bush") long: Long, int: Int)
```

Would generate this schema:

```json
{
  "type": "record",
  "name": "Annotated",
  "namespace": "com.sksamuel.avro4s.schema",
  "fields": [
    {
      "name": "str",
      "type": "string",
      "richard": "ashcroft"
    },
    {
      "name": "long",
      "type": "long",
      "kate": "bush"
    },
    {
      "name": "int",
      "type": "int"
    }
  ],
  "jack": "bruce"
}
```

### Overriding a Schema

Behind the scenes, `AvroSchema` uses an implicit `SchemaFor`. This is the core typeclass which generates an Avro schema for a given Java or Scala type.
There are `SchemaFor` instances for all the common JDK and SDK types, as well as macros that generate instances for case classes.

In order to override how a schema is generatef you need to bring into scope an implicit `SchemaFor` for the type you want to override.
As an example, lets say you wanted all Strings to be encoded as `Schema.Type.BYTES` rather than the standard `Schema.Type.STRING`.
Then we could create an implicit like this:

```scala
implicit object MyStringOverride extends SchemaFor[String] {
  override def schema: Schema = SchemaBuilder.builder.bytesType
}

case class Foo(a: String)

val schema = AvroSchema[Foo]
```

Note: If you create an override like this, be aware that schemas in Avro are mutable, so don't share instances of the schemas.

### Recursive Schemas

Avro4s supports recursive schemas, but you will have to manually force the `SchemaFor` instance, instead of letting it be generated.

``` scala
case class Recursive(payload: Int, next: Option[Recursive])

implicit val schemaFor = SchemaFor[Recursive]
val schema = AvroSchema[Recursive]
```

## Input / Output

### Serializing

Avro4s allows us to easily serialize case classes using an instance of `AvroOutputStream` which we write to, and close, just like you would any regular output stream.
An `AvroOutputStream` can be created from a `File`, `Path`, or by wrapping another `OutputStream`.
When we create one, we specify the type of objects that we will be serializing and provide a writer schema.
For example, to serialize instances of our Pizza class:

```scala
import java.io.File
import com.sksamuel.avro4s.AvroOutputStream

val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 598)
val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, false, 391)

val schema = AvroSchema[Pizza]

val os = AvroOutputStream.data[Pizza].to(new File("pizzas.avro")).build(schema)
os.write(Seq(pepperoni, hawaiian))
os.flush()
os.close()
```

### Deserializing

We can easily deserialize a file back into case classes.
Given the `pizzas.avro` file we generated in the previous section on serialization, we will read this back in using the `AvroInputStream` class.
We first create an instance of the input stream specifying the types we will read back, the source file, and then build it using a reader schema.

Once the input stream is created, we can invoke `iterator` which will return a lazy iterator that reads on demand the data in the file.

In this example, we'll load all data at once from the iterator via `toSet`.

```scala
import com.sksamuel.avro4s.AvroInputStream

val schema = AvroSchema[Pizza]

val is = AvroInputStream.data[Pizza].from(new File("pizzas.avro")).build(schema)
val pizzas = is.iterator.toSet
is.close()

println(pizzas.mkString("\n"))
```

Will print out:

```scala
Pizza(pepperoni,List(Ingredient(pepperoni,12.2,4.4), Ingredient(onions,1.2,0.4)),false,false,500)
Pizza(hawaiian,List(Ingredient(ham,1.5,5.6), Ingredient(pineapple,5.2,0.2)),false,false,500)
```

### Binary and JSON Formats
You can serialize as [binary](https://avro.apache.org/docs/1.8.2/spec.html#binary_encoding) or [json](https://avro.apache.org/docs/1.8.2/spec.html#json_encoding)
by specifying the format when creating the input or output stream. In the earlier example we use `data` which is considered the "default" for Avro.

To use json or binary, you can do the following:

```scala
AvroOutputStream.binary.to(...).build(...)
AvroOutputStream.json.to(...).build(...)

AvroInputStream.binary.from(...).build(...)
AvroInputStream.json.from(...).build(...)
```

Note: Binary serialization does not include the schema in the output.


## Avro Records

In Avro there are two container interfaces designed for complex types - `GenericRecord`, which is the most commonly used, along with the lesser used `SpecificRecord`.
These record types are used with a schema of type `Schema.Type.RECORD`.

To interface with the Avro Java API or with third party frameworks like Kafka it is sometimes desirable to convert between your case classes and these records,
rather than using the input/output streams that avro4s provides.

To perform conversions, use the `RecordFormat` typeclass which converts to/from case classes and Avro records.

Note: In Avro, `GenericRecord` and `SpecificRecord` don't have a common _Record_ interface (just a `Container` interface which simply provides for a schema without any methods for accessing values), so
avro4s has defined a `Record` trait, which is the union of the `GenericRecord` and `SpecificRecord` interfaces. This allows avro4s to generate records which implement both interfaces at the same time.

To convert from a class into a record:

```scala
case class Composer(name: String, birthplace: String, compositions: Seq[String])
val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
val format = RecordFormat[Composer]
// record is a type that implements both GenericRecord and Specific Record
val record = format.to(ennio)
```

And to go from a record back into a type:

```scala
// given some record from earlier
val record = ...
val format = RecordFormat[Composer]
val ennio = format.from(record)
```

## Type Mappings

Avro4s defines two typeclasses, `Encoder` and `Decoder` which do the work
of mapping between scala values and Avro compatible values. There are built in encoders and decoders for all the common types.

For example, given a string, and a schema of type `Schema.Type.STRING` then the default encoder would return an instance of `Utf8`, whereas
the same string and a `Schema.Type.FIXED` schema would be encoded as an instance of `GenericData.Fixed`.

Another example is given an `Option[T]` then an instance of `None` would be encoded as null, and an instance of `Some` would be
encoded as the underlying value.

Decoders do the same work, but in reverse. They take an Avro value, such as null and return a scala value, such as `Option`.

### Built in Type Mappings

``` scala
import scala.collection.{Array, List, Seq, Iterable, Set, Map, Option, Either}
import shapeless.{:+:, CNil}
```

| Scala Type                   	| Schema Type   	| Logical Type     	|
|------------------------------	|---------------	|------------------	|
| String                       	| STRING        	|                  	|
| Boolean                      	| BOOLEAN       	|                  	|
| Long                         	| LONG          	|                  	|
| Int                          	| INT           	|                  	|
| Short                        	| INT           	|                  	|
| Byte                         	| INT           	|                  	|
| Double                       	| DOUBLE        	|                  	|
| Float                        	| FLOAT         	|                  	|
| UUID                         	| STRING        	| UUID             	|
| LocalDate                    	| INT           	| Date             	|
| LocalTime                    	| INT           	| Time-Millis      	|
| LocalDateTime                	| LONG          	| Timestamp-Millis 	|
| java.sql.Date                	| INT           	| Date             	|
| Instant                      	| LONG          	| Timestamp-Millis 	|
| Timestamp                    	| LONG          	| Timestamp-Millis 	|
| BigDecimal                   	| BYTES         	| Decimal<8,2>     	|
| Option[T]                    	| UNION<null,T> 	|                  	|
| Array[Byte]                  	| BYTES         	|                  	|
| ByteBuffer                   	| BYTES         	|                  	|
| Seq[Byte]                    	| BYTES         	|                  	|
| List[Byte]                   	| BYTES         	|                  	|
| Vector[Byte]                 	| BYTES         	|                  	|
| Array[T]                     	| ARRAY<T>      	|                  	|
| Vector[T]                    	| ARRAY<T>      	|                  	|
| Seq[T]                       	| ARRAY<T>      	|                  	|
| List[T]                      	| ARRAY<T>      	|                  	|
| Set[T]                       	| ARRAY<T>      	|                  	|
| sealed trait of case classes 	| UNION<A,B>    	|                  	|
| sealed trait of case objects 	| ENUM<A,B>     	|                  	|
| Map[String, V]              	| MAP<V>        	|                  	|
| Either[A,B]                  	| UNION<A,B>    	|                  	|
| A :+: B :+: C :+: CNil       	| UNION<A,B,C>  	|                  	|
| case class T                 	| RECORD        	|                  	|
| Scala enumeration            	| ENUM          	|                  	|
| Java enumeration             	| ENUM          	|                  	|

### Custom Type Mappings

It is very easy to add custom type mappings. To do this, we bring into scope a custom implicit of `Encoder[T]` and/or `Decoder[T]`.

For example, to create a custom type mapping for a type Foo which writes out the contents in upper case, but always reads
the contents in lower case, we can do the following:

```scala
case class Foo(a: String, b: String)

implicit object FooEncoder extends Encoder[Foo] {
  override def encode(foo: Foo, schema: Schema): AnyRef = {
    val record = new GenericData.Record(schema)
    record.put("a", foo.a.toUpperCase)
    record.put("b", foo.b.toUpperCase)
    record
  }
}

implicit object FooDecoder extends Decoder[Foo] {
  override def decode(value: Any, schema: Schema): Foo = {
    val record = value.asInstanceOf[GenericRecord]
    Foo(record.get("a").toString.toLowerCase, record.get("b").toString.toLowerCase)
  }
}
```

Another example is changing the way we serialize `LocalDateTime` to store these dates as ISO strings. In this case, we are
also changing the schema type from the default LONG to STRING, so we must add an implicit `SchemaFor` as well as the encoders
and decoders.

```scala
implicit object DateTimeSchemaFor extends SchemaFor[LocalDateTime] {
  override val schema: Schema = Schema.create(Schema.Type.STRING)
}

implicit object DateTimeEncoder extends Encoder[LocalDateTime] {
  override def apply(value: LocalDateTime, schema: Schema): AnyRef = ISODateTimeFormat.dateTime().print(value)
}

implicit object DateTimeDecoder extends Decoder[LocalDateTime] {
  override def apply(value: Any, field: Field): LocalDateTime = ISODateTimeFormat.dateTime().parseDateTime(value.toString)
}
```

These typeclasses must be implicit and in scope when you use `AvroSchema` or `RecordFormat`.

### Coproducts

Avro supports generalised unions, eithers of more than two values.
To represent these in scala, we use `shapeless.:+:`, such that `A :+: B :+: C :+: CNil` represents cases where a type is `A` OR `B` OR `C`.
See shapeless' [documentation on coproducts](https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-2.0.0#coproducts-and-discriminated-unions) for more on how to use coproducts.

### Sealed hierarchies

Scala sealed traits/classes are supported both when it comes to schema generation and conversions to/from `GenericRecord`.
Generally sealed hierarchies are encoded as unions - in the same way like Coproducts.
Under the hood, shapeless `Generic` is used to derive Coproduct representation for sealed hierarchy.

When all descendants of sealed trait/class are singleton objects, optimized, enum-based encoding is used instead.


## Decimal scale, precision and rounding mode

Bring an implicit `ScalePrecisionRoundingMode` into scope before using `AvroSchema`.

```scala
import com.sksamuel.avro4s.ScalePrecisionRoundingMode

case class MyDecimal(d: BigDecimal)

implicit val sp = ScalePrecisionRoundingMode(8, 20, RoundingMode.HALF_UP)
val schema = AvroSchema[MyDecimal]
```

```json
{
  "type":"record",
  "name":"MyDecimal",
  "namespace":"com.foo",
  "fields":[{
    "name":"d",
    "type":{
      "type":"bytes",
      "logicalType":"decimal",
      "scale":"8",
      "precision":"20"
    }
  }]
}
```

### Type Parameters

When serializing a class with one or more type parameters, the avro name used in a schema is the name of the raw type, plus the actual type parameters. In other words, it would be of the form `rawtype__typeparam1_typeparam2_..._typeparamN`. So for example, the schema for a type `Event[Foo]` would have the avro name `event__foo`.

You can disable this by annotating the class with `@AvroErasedName` which uses the JVM erased name - in other words, it drops type parameter information. So the aforementioned `Event[Foo]` would be simply `event`.

### Selective Customisation

You can selectively customise the way Avro4s generates certain parts of your hierarchy, thanks to implicit precedence. Suppose you have the following classes:

```scala
case class Product(name: String, price: Price, litres: BigDecimal)
case class Price(currency: String, amount: BigDecimal)
```

And you want to selectively use different scale/precision for the `price` and `litres` quantities. You can do this by forcing the implicits in the corresponding companion objects.

``` scala
object Price {
  implicit val sp = ScalePrecisionRoundingMode(10, 2, scala.math.BigDecimal.RoundingMode.UNNECESSARY)
  implicit val schema = SchemaFor[Price]
}

object Product {
  implicit val sp = ScalePrecisionRoundingMode(8, 4, scala.math.BigDecimal.RoundingMode.UNNECESSARY)
  implicit val schema = SchemaFor[Product]
}
```

This will result in a schema where both `BigDecimal` quantities have their own separate scale and precision.


## Using avro4s in your project

#### Gradle

`compile 'com.sksamuel.avro4s:avro4s-core_2.12:xxx'`

#### SBT

`libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "xxx"`

#### Maven

```xml
<dependency>
    <groupId>com.sksamuel.avro4s</groupId>
    <artifactId>avro4s-core_2.12</artifactId>
    <version>xxx</version>
</dependency>
```

Check the latest released version on [Maven Central](http://search.maven.org/#search|ga|1|g%3A%22com.sksamuel.avro4s%22)

## Contributions
Contributions to avro4s are always welcome. Good ways to contribute include:

* Raising bugs and feature requests
* Fixing bugs and enhancing the DSL
* Improving the performance of avro4s
* Adding to the documentation
