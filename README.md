# ![logo.png](logo.png)

![build](https://github.com/sksamuel/avro4s/workflows/master/badge.svg)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avro4s/avro4s-core_2.12.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Cavro4s-core_2.12)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avro4s/avro4s-core_2.13.svg?label=latest%20release%20for%202.13"/>](http://search.maven.org/#search%7Cga%7C1%7Cavro4s-core_2.13)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avro4s/avro4s-core_3.svg?label=latest%20release%20for%203.0"/>](http://search.maven.org/#search%7Cga%7C1%7Cavro4s-core_3)
[<img src="https://img.shields.io/nexus/s/https/oss.sonatype.org/com.sksamuel.avro4s/avro4s-core_3.svg?label=latest%20snapshot&style=plastic"/>](https://oss.sonatype.org/content/repositories/snapshots/com/sksamuel/avro4s/)

** This project is in maintaince only mode - PRs will be accepted and releases published but active development has ceased **

Avro4s is a schema/class generation and serializing/deserializing library for [Avro](http://avro.apache.org/) written in Scala. The objective is to allow seamless use with Scala without the need to write boilerplate conversions yourself, and without the runtime overhead of reflection. Hence, this is a macro based library and generates code for use with Avro at _compile time_.

The features of the library are:
* Schema generation from classes at compile time
* Boilerplate free serialization of Scala types into Avro types
* Boilerplate free deserialization of Avro types to Scala types

## Versioning

The `master` branch contains version 5.0.x which is designed for Scala 3. PRs are welcome. This version may have minor breaking changes compared to previous major release in order to support the new features of Scala 3.

The previous major version is 4.0.x located at branch `release/4.0.x` and is released for Scala 2.12 and Scala 2.13.
This version is in support mode only. Bug reports are welcome and bug fixes will be released. No new features will be
added.

Please raise PRs using branch names `scala2/*` and `scala3/*` depending on which version of Scala your work is
targeting.

## Schemas

Unlike Json, Avro is a schema based format. You'll find yourself wanting to generate schemas frequently, and writing
these by hand or through the Java based `SchemaBuilder` classes can be tedious for complex domain models. Avro4s allows
us to generate schemas directly from case classes at compile time via macros. This gives you both the convenience of
generated code, without the annoyance of having to run a code generation step, as well as avoiding the peformance
penalty of runtime reflection based code.

Let's define some classes.

```scala
case class Ingredient(name: String, sugar: Double, fat: Double)
case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
```

To generate an Avro Schema, we need to use the `AvroSchema` object passing in the target type as a type parameter.
This will return an `org.apache.avro.Schema` instance.

```scala
import com.sksamuel
val schema = AvroSchema[Pizza]
```

Where the generated schema is as follows:

```json
{
   "type":"record",
   "name":"Pizza",
   "namespace":"com.sksamuel",
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
package com.sksamuel
case class Foo(a: String)
```

Would normally have a schema like this:

```json
{
  "type":"record",
  "name":"Foo",
  "namespace":"com.sksamuel",
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
package com.sksamuel

@AvroName("Wibble")
@AvroNamespace("com.other")
case class Foo(a: String)
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

Note: It is possible, but not necessary, to use both AvroName and AvroNamespace. You can just use either of them if you wish.

### Overriding a field name

The `AvroName` annotation can also be used to override field names. This is useful when the record instances you are generating or reading need to have field names different from the scala case classes. For example if you are reading data generated by another system, or another language.

Given the following class.

```scala
package com.sksamuel
case class Foo(a: String, @AvroName("z") b : String)
```

Then the generated schema would look like this:

```json
{
  "type":"record",
  "name":"Foo",
  "namespace":"com.sksamuel",
  "fields":[
    {
      "name":"a",
      "type":"string"
    },
    {
      "name":"z",
      "type":"string"
    }    
  ]
}
```

Notice that the second field is `z` and not `b`.

Note: @AvroName does not add an alternative name for the field, but an override. If you wish to have alternatives then you want to use @AvroAlias.

### Adding properties and docs to a Schema

Avro allows a doc field, and arbitrary key/values to be added to generated schemas. Avro4s supports this through the use of `AvroDoc` and `AvroProp` annotations.

These properties works on either complex or simple types - in other words, on both fields and classes. For example:

```scala
package com.sksamuel
@AvroDoc("hello, is it me you're looking for?")
case class Foo(@AvroDoc("I am a string") str: String, @AvroDoc("I am a long") long: Long, int: Int)
```

Would result in the following schema:

```json
{  
  "type": "record",
  "name": "Foo",
  "namespace": "com.sksamuel",
  "doc":"hello, is it me you're looking for?",
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
  "namespace": "com.sksamuel",
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

Behind the scenes, `AvroSchema` uses an implicit `SchemaFor`. This is the core typeclass which generates an Avro schema for a given Java or Scala type. There are `SchemaFor` instances for all the common JDK and SDK types, as well as macros that generate instances for case classes.

In order to override how a schema is generated for a particular type you need to bring into scope an implicit `SchemaFor` for the type you want to override. As an example, lets say you wanted all integers to be encoded as `Schema.Type.STRING` rather than the standard `Schema.Type.INT`.

To do this, we just introduce a new instance of `SchemaFor` and put it in scope when we generate the schema.

```scala
implicit val intOverride = SchemaFor[Int](SchemaBuilder.builder.stringType)

case class Foo(a: Int)
val schema = AvroSchema[Foo]
```

Note: If you create an override like this, be aware that schemas in Avro are mutable, so don't share the values that the typeclasses return.

### Transient Fields

Avro4s does not support the @transient anotation to mark a field as ignored, but instead supports its own @AvroTransient annotation to do the same job. Any field marked with this will be excluded from the generated schema.

```scala
package com.sksamuel
case class Foo(a: String, @AvroTransient b: String)
```

Would result in the following schema:

```json
{  
  "type": "record",
  "name": "Foo",
  "namespace": "com.sksamuel",
  "fields": [  
    {  
      "name": "a",
      "type": "string"
    }
  ]
}
```

### Field Mapping

If you are dealing with Avro data generated in other languages then it's quite likely the field names will reflect the style of that language. For example, Java may prefer `camelCaseFieldNames` but other languages may use `snake_case_field_names` or `PascalStyleFieldNames`. By default the name of the field in the case class is what will be used, and you've seen earlier that you can override a specific field with @AvroName, but doing this for every single field would be insane.

So, avro4s provides a `FieldMapper` for this. You simply bring into scope an instance of `FieldMapper` that will convert the scala field names into a target type field names.

For example, lets take a scala case and generate a schema using snake case.

```scala
package com.sksamuel
case class Foo(userName: String, emailAddress: String)
implicit val snake: FieldMapper = SnakeCase
val schema = AvroSchema[Foo]
```

Would generate the following schema:

```json
{
  "type": "record",
  "name": "Foo",
  "namespace": "com.sksamuel",
  "fields": [
    {
      "name": "user_name",
      "type": "string"
    },
    {
      "name": "email_address",
      "type": "string"
    }
  ]
}
```

You can also define your own field mapper:

```scala
package com.sksamuel
case class Foo(userName: String, emailAddress: String)
implicit val short: FieldMapper = {
  case "userName"     => "user"
  case "emailAddress" => "email"
}
val schema = AvroSchema[Foo]
```

Would generate the following schema:

```json
{
  "type": "record",
  "name": "Foo",
  "namespace": "com.sksamuel",
  "fields": [
    {
      "name": "user",
      "type": "string"
    },
    {
      "name": "email",
      "type": "string"
    }
  ]
}
```

### Field Defaults

Avro4s will take into account default values on fields. For example, the following class `case class Wibble(s: String = "foo")` would be serialized as:

```json
{
  "type": "record",
  "name": "Wibble",
  "namespace": "com.sksamuel.avro4s.schema",
  "fields": [
    {
      "name": "s",
      "type": "string",
      "default" : "foo"
    }
  ]
}
```

However if you wish the scala default to be ignored, then you can annotate the field with @AvroNoDefault. So this class `case class Wibble(@AvroNoDefault s: String = "foo")` would be serialized as:

```json
{
  "type": "record",
  "name": "Wibble",
  "namespace": "com.sksamuel.avro4s.schema",
  "fields": [
    {
      "name": "s",
      "type": "string"
    }
  ]
}
```

### Enums and Enum Defaults

#### AVRO Enums from Scala Enums, Java Enums, and Sealed Traits

Avro4s maps scala enums, java enums, and scala sealed traits to the AVRO `enum` type.
For example, the following scala enum:
```scala
object Colours extends Enumeration {
  val Red, Amber, Green = Value
}
```
when referenced in a case class:
```scala
case class Car(colour: Colours.Value)
```
results in the following AVRO schema (e.g. using `val schema = AvroSchema[Car]`):
```json
{
  "type" : "record",
  "name" : "Car",
  "fields" : [ {
    "name" : "colour",
    "type" : {
      "type" : "enum",
      "name" : "Colours",
      "symbols" : [ "Red", "Amber", "Green" ]
    }
  } ]
}
```
Avro4s will also convert a Java enum such as:
```java
public enum Wine {
    Malbec, Shiraz, CabSav, Merlot
}
```
into an AVRO `enum` type:
```json
{
  "type": "enum",
  "name": "Wine",
  "symbols": [ "Malbec", "Shiraz", "CabSav", "Merlot" ]
}
```
And likewise, avro4s will convert a sealed trait such as:
```scala
sealed trait Animal
@AvroSortPriority(0) case object Cat extends Animal
@AvroSortPriority(-1) case object Dog extends Animal
```
into the following AVRO `enum` schema:
```json
{
  "type" : "enum",
  "name" : "Animal",
  "symbols" : [ "Cat", "Dog" ]
}
```

With `@AvroSortPriority` attribute, elements are sorted in descending order, by the priority specified
(the element with the highest priority will be put as first).

According to Avro specification, when an element is not found the first compatible element defined in the union is used.
For this reason order of the elements should not be changed when compatibility is important.
Add new elements at the end.

An alternative solution is to use the `@AvroUnionPosition` attribute passing a number that will be sorted ascending,
from lower to upper:

```scala
  sealed trait Fruit
  @AvroUnionPosition(0)
  case object Unknown extends Fruit
  @AvroUnionPosition(1)
  case class Orange(size: Int) extends Fruit
  @AvroUnionPosition(2)
  case class Mango(size: Int) extends Fruit
```

This will generate the following AVRO schema:
```json
[
    {
        "type" : "record",
        "name" : "Unknown",
        "fields" : [ ]
    },
    {
        "type" : "record",
        "name" : "Orange",
        "fields" : [ {
            "name" : "size",
            "type" : "int"
        } ]
    },
    {
        "type" : "record",
        "name" : "Mango",
        "fields" : [ {
            "name" : "size",
            "type" : "int"
        } ]
    }
]
``` 

#### Field Defaults vs. Enum Defaults

As with any AVRO field, you can specify an enum field's default value as follows:
```scala
case class Car(colour: Colours.Value = Colours.Red)
```
resulting in the following AVRO schema:
```json
{
  "type" : "record",
  "name" : "Car",
  "fields" : [ {
    "name" : "colour",
    "type" : {
      "type" : "enum",
      "name" : "Colours",
      "symbols" : [ "Red", "Amber", "Green" ]
    },
    "default": "Red"
  } ]
}
```
One benefit of providing a field default is that the writer can later remove the field without
breaking existing readers. In the `Car` example, if the writer doesn't provide a value for the
`colour` field, the reader will default the `colour` to `Red`.

But what if the writer would like to extend the `Colour` enumeration to include the colour `Orange`:
```scala
object Colours extends Enumeration {
  val Red, Amber, Green, Orange = Value
}
```
resulting in the following AVRO schema?
```json
{
  "type" : "record",
  "name" : "Car",
  "fields" : [ {
    "name" : "colour",
    "type" : {
      "type" : "enum",
      "name" : "Colours",
      "symbols" : [ "Red", "Amber", "Green", "Orange" ]
    },
    "default": "Red"
  } ]
}
```
If a writer creates an `Orange` `Car`:
```scala
Car(colours = Colours.Orange)
```
readers using the older schema (the one without the new `Orange` value), will fail with a backwards compatibility error.
I.e. readers using the previous version of the `Car` schema don't know the colour `Orange`, and therefore
can't read the new `Car` record.

To enable writers to extend enums in a backwards-compatible way, AVRO allows you to specify a default enum value 
as part of the enum type's definition:
```json
{
  "type" : "enum",
  "name" : "Colours",
  "symbols" : [ "Red", "Amber", "Green" ],
  "default": "Amber"
}
```
Note that an enum's default isn't the same as an enum field's default as showed below,
where the enum default is `Amber` and the field's default is `Red`:
```json
{
  "type" : "record",
  "name" : "Car",
  "fields" : [ {
    "name" : "colour",
    "type" : {
      "type" : "enum",
      "name" : "Colours",
      "symbols" : [ "Red", "Amber", "Green" ],
      "default": "Amber"
    },
    "default": "Red"
  } ]
}
```
Note that the field's default and the enum's default need not be the same value.

The field's default answers the question:
* What value should the reader use if the writer didn't specify the field's value?

In the schema example above, the answer is `Red`.

The enum's default value answers the question:
* What value should the reader use if the writer specifies an enum value that the reader doesn't recognize?

In the example above, the answer is `Amber`.

In summary, as long as a writer specified a the default enum value in previous versions of an enum's schema, the writer can add
new enum values without breaking older readers. For example, we can add
the colour `Orange` to the `Colour` enum's list of symbol/values without breaking older readers:
```json
{
  "type" : "record",
  "name" : "Car",
  "fields" : [ {
    "name" : "colour",
    "type" : {
      "type" : "enum",
      "name" : "Colours",
      "symbols" : [ "Red", "Amber", "Green", "Orange" ],
      "default": "Amber"
    },
    "default": "Red"
  } ]
}
```
Specifically, given `Amber` as the enum's default, an older AVRO reader that receives an `Orange` `Car` will 
default the `Car`'s `colour` to `Amber`, the enum's default.

The following sections describe how to define enum defaults through avro4s for scala enums, java enums,
and sealed traits.

#### Defining Enum Defaults for Scala Enums

For scala enums such as:
```scala
object Colours extends Enumeration {
   val Red, Amber, Green = Value
}
 ```
avro4s gives you two options:
1) You can define an implicit `SchemaFor` using the `ScalaEnumSchemaFor[E].apply(default: E)` method
where the method's `default` argument is one of the enum's values or ...
2) You can use the `@AvroEnumDefault` annotation to declare the default enum value.

For example, to create an implicit `SchemaFor` for an scala enum with a default enum value, 
use the `ScalaEnumSchemaFor[E].apply(default: E)` method as follows:
```scala
implicit val schemaForColours: SchemaFor[Colours.Value] = ScalaEnumSchemaFor[Colours.Value](default = Colours.Amber)
```
resulting in the following AVRO schema:
```json
{
  "type" : "enum",
  "name" : "Colours",
  "symbols" : [ "Red", "Amber", "Green" ],
  "default": "Amber"
}
```
Or, to declare the default enum value, you can use the `@AvroEnumDefault` annotation as follows:
```scala
@AvroEnumDefault(Colours.Amber)
object Colours extends Enumeration {
   val Red, Amber, Green = Value
}
```
resulting in the same AVRO schema:
```json
{
  "type" : "enum",
  "name" : "Colours",
  "symbols" : [ "Red", "Amber", "Green" ],
  "default": "Amber"
}
```
You can also use the following avro4s annotations to change a scala enum's name, namespace, and to add additional properties:
* `@AvroName`
* `@AvroNamespace`
* `@AvroProp`

For example:
```scala
@AvroName("MyColours")
@AvroNamespace("my.namespace")
@AvroEnumDefault(Colours.Green)
@AvroProp("hello", "world")
object Colours extends Enumeration {
  val Red, Amber, Green = Value
}
```
resulting in the following AVRO schema:
```json
{
  "type" : "enum",
  "name" : "MyColours",
  "namespace" : "my.namespace",
  "symbols" : [ "Red", "Amber", "Green" ],
  "default": "Amber",
  "hello" : "world"
}
```
Note that if you're using an enum from, for example, a 3rd party library and without access to the source code, you may
not be able to use the `@AvroEnumDefault` annotation, in which case you'll need to use the
`ScalaEnumSchemaFor[E].apply(default: E)` method instead.

#### Defining Enum Defaults for Java Enums

For java enums such as:
```java
public enum Wine {
  Malbec, 
  Shiraz, 
  CabSav, 
  Merlot
}
```
avro4s gives you two options to define an enum's default value:
1) You can define an implicit `SchemaFor` using the `JavaEnumSchemaFor[E].apply(default: E)` method
where the method's `default` argument is one of the enum's values or ...
2) You can use the `@AvroJavaEnumDefault` annotation to declare the default enum value.

For example, to create an implicit `SchemaFor` for an enum with a default enum value,
use the `JavaEnumSchemaFor[E].apply(default: E)` method as follows:
```scala
implicit val schemaForWine: SchemaFor[Wine] = JavaEnumSchemaFor[Wine](default = Wine.Merlot)
```
Or, to declare the default enum value, use the `@AvroJavaEnumDefault` annotation as follows:
```java
public enum Wine {
  Malbec, 
  Shiraz, 
  @AvroJavaEnumDefault CabSav,
  Merlot
}
```
Avro4s also supports the following java annotations for java enums:
* `@AvroJavaName`
* `@AvroJavaNamespace`
* `@AvroJavaProp`

Putting it all together, you can define a java enum with using avro4s's annotations as follows:
```java
@AvroJavaName("MyWine")
@AvroJavaNamespace("my.namespace")
@AvroJavaProp(key = "hello", value = "world")
public enum Wine {
  Malbec, 
  Shiraz, 
  @AvroJavaEnumDefault CabSav,
  Merlot
}
```
resulting in the following AVRO schema:
```json
{
  "type": "enum",
  "name": "MyWine",
  "namespace": "my.namespace",
  "symbols": [
    "Malbec",
    "Shiraz",
    "CabSav",
    "Merlot"
  ],
  "default": "CabSav",
  "hello": "world"
}
```
#### Defining Enum Defaults for Sealed Traits

For sealed traits, you can define the trait's default enum using the `@AvroEnumDefault` annotation as follows:
```scala
@AvroEnumDefault(Dog)
sealed trait Animal
@AvroSortPriority(0) case object Cat extends Animal
@AvroSortPriority(-1) case object Dog extends Animal
```
resulting in the following AVRO schema:
```json
{
  "type" : "enum",
  "name" : "Animal",
  "symbols" : [ "Cat", "Dog" ],
  "default" : "Dog"
}
```

### Avro Fixed

Avro supports the idea of fixed length byte arrays. To use these we can either override the schema generated for a type to return `Schema.Type.Fixed`. This will work for types like String or UUID. You can also annotate a field with @AvroFixed(size).
For example:

```scala
package com.sksamuel
case class Foo(@AvroFixed(7) mystring: String)
val schema = AvroSchema[Foo]
```

Will generate the following schema:

```json
{
  "type": "record",
  "name": "Foo",
  "namespace": "com.sksamuel",
  "fields": [
    {
      "name": "mystring",
      "type": {
        "type": "fixed",
        "name": "mystring",
        "size": 7
      }
    }
  ]
}
```

If you have a value type that you always want to be represented as fixed, then rather than annotate every single location it is used, you can annotate the value type itself.

```scala
package com.sksamuel

@AvroFixed(4)
case class FixedA(bytes: Array[Byte]) extends AnyVal

case class Foo(a: FixedA)
val schema = AvroSchema[Foo]
```

And this would generate:

```json
{
  "type": "record",
  "name": "Foo",
  "namespace": "com.sksamuel",
  "fields": [
    {
      "name": "a",
      "type": {
        "type": "fixed",
        "name": "FixedA",
        "size": 4
      }
    }
  ]
}
```

Finally, these annotated value types can be used as top level schemas too:

```scala
package com.sksamuel

@AvroFixed(6)
case class FixedA(bytes: Array[Byte]) extends AnyVal
val schema = AvroSchema[FixedA]
```

```json
{
  "type": "fixed",
  "name": "FixedA",
  "namespace": "com.sksamuel",
  "size": 6
}
```

### Controlling order of types in generated union schemas

The order of types in a union is significant in Avro, e.g the schemas `type: ["int", "float"]` and `type: ["float", "int"]` are different. This can cause problems when generating schemas for sealed trait hierarchies. Ideally we would generate schemas using the source code declaration order of the types. So for example:

```scala
sealed trait Animal
case class Dog(howFriendly: Float) extends Animal
case class Fish(remembersYou: Boolean) extends Animal
```

Should generate a schema where the order of types in the unions is `Dog, Fish`. Unfortunately, the `SchemaFor` macro can sometimes lose track of what the declaration order is - especially with larger hierarchies. In any situation where this is happening you can use the `@AvroSortPriority` annotation to explicitly control what order the types appear in. `@AvroSortPriority` takes a single float argument, which is the priority this field should be treated with, higher priority means closer to the beginning of the union. For example:

```scala
sealed trait Animal
@AvroSortPriority(1)
case class Dog(howFriendly: Float) extends Animal
@AvroSortPriority(2)
case class Fish(remembersYou: Boolean) extends Animal
```

Would output the types in the union as `Fish,Dog`.

### Recursive Schemas

Avro4s supports recursive schemas. Customizing them requires some thought, so if you can stick with the out-of-the-box
provided schemas and customization via annotations. 

### Customizing Recursive Schemas

The simplest way to customize schemas for recursive types is to provide custom `SchemaFor` instances for all types that
form the recursion. Given for example the following recursive `Tree` type,

```scala
sealed trait Tree[+T]
case class Branch[+T](left: Tree[T], right: Tree[T]) extends Tree[T]
case class Leaf[+T](value: T) extends Tree[T]
```

it is easy to customize recursive schemas by providing a `SchemaFor` for both `Tree` and `Branch`:

```scala
import scala.collection.JavaConverters._

val leafSchema = AvroSchema[Leaf[Int]]
val branchSchema = Schema.createRecord("CustomBranch", "custom schema", "custom", false)
val treeSchema = Schema.createUnion(leafSchema, branchSchema)
branchSchema.setFields(Seq(new Schema.Field("left", treeSchema), new Schema.Field("right", treeSchema)).asJava)

val treeSchemaFor: SchemaFor[Tree[Int]] = SchemaFor(treeSchema)
val branchSchemaFor: SchemaFor[Branch[Int]] = SchemaFor(branchSchema)
```

If you want to customize the schema for one type that is part of a type recursion (e.g., `Branch[Int]`) while using
generated schemas, this can be done as follows (sticking with the above example):

```scala
// 1. Use implicit def here so that this SchemaFor gets summoned for Branch[Int] in steps 6. and 10. below
// 2. Implement a ResolvableSchemaFor instead of SchemaFor directly so that SchemaFor creation can be deferred
implicit def branchSchemaFor: SchemaFor[Branch[Int]] = new ResolvableSchemaFor[Branch[Int]] {
  def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[Branch[Int]] =
    // 3. first, check whether SchemaFor[Branch[Int]] is already defined and return that if it is 
    env.get[Branch[Int]].getOrElse {
      // 4. otherwise, create an incomplete SchemaFor (it initially lacks fields)
      val record: SchemaFor[Branch[Int]] = SchemaFor(Schema.createRecord("CustomBranch", "custom schema", "custom", false))
      // 5. extend the definition environment with the created SchemaFor[Branch[Int]]
      val nextEnv = env.updated(record)
      // 6. summon a schema for Tree[Int] (using the Branch[Int] from step 1. through implicits)
      // 7. resolve the schema to get a finalized schema for Tree[Int]
      val treeSchema = SchemaFor[Tree[Int]].resolveSchemaFor(nextEnv, NoUpdate).schema
      // 8. close the reference cycle between Branch[Int] and Tree[Int]
      val fields = Seq(new Schema.Field("left", treeSchema), new Schema.Field("right", treeSchema))
      record.schema.setFields(fields.asJava)
      // 9. return the final SchemaFor[Branch[Int]]
      record
    }
}

// 10. summon Schema for tree and kick off encoder resolution.
val treeSchema = AvroSchema[Tree[Int]]
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

val os = AvroOutputStream.data[Pizza].to(new File("pizzas.avro")).build()
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
val schema: Schema = AvroSchema[Composer]
implicit val toRecord: ToRecord[Composer] = ToRecord.apply[Composer](schema)
implicit val fromRecord: FromRecord[Composer] = FromRecord.apply[Composer](schema)
val format: RecordFormat[Composer] = RecordFormat.apply[Composer](schema)
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

## Usage as a Kafka Serde

The [com.sksamuel.avro4s.kafka.GenericSerde](avro4s-kafka/src/main/scala/com/sksamuel/avro4s/kafka/GenericSerde.scala) class can be used as a Kafka Serdes to serialize/deserialize case classes into Avro records with Avro4s.
Note that this class is not integrated with the schema registry.

```scala

  import java.util.Properties
  import org.apache.kafka.clients.CommonClientConfigs
  import org.apache.kafka.clients.producer.ProducerConfig
  import com.sksamuel.avro4s.BinaryFormat

  case class TheKafkaKey(id: String)
  case class TheKafkaValue(name: String, location: String)

  val producerProps = new Properties();
  producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "...")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new GenericSerde[TheKafkaKey](BinaryFormat))
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new GenericSerde[TheKafkaValue](BinaryFormat))
  new ProducerConfig(producerProps)
```

## Type Mappings

Avro4s defines two typeclasses, `Encoder` and `Decoder` which do the work
of mapping between scala values and Avro compatible values. Avro has no understanding of Scala types, or anything outside of it's built in set of supported types, so all values must be converted to something that is compatible with Avro. There are built in encoders and decoders for all the common JDK and Scala SDK types, including macro generated instances for case classes.

For example a `java.sql.Timestamp` is usually encoded as a Long, and a `java.util.UUID` is encoded as a String.

Decoders do the same work, but in reverse. They take an Avro value, such as null and return a scala value, such as `Option`.

Some values can be mapped in multiple ways depending on how the schema was generated. For example a String, which is usually encoded as 
`org.apache.avro.util.Utf8` could also be encoded as an array of bytes if the generated schema for that field was `Schema.Type.BYTES`. Therefore some encoders will take into account the schema passed to them when choosing the avro compatible type. In the schemas section you saw how you could influence which schema is generated for types.

### Built in Type Mappings

``` scala
import scala.collection.{Array, List, Seq, Iterable, Set, Map, Option, Either}
import shapeless.{:+:, CNil}
```

The following table shows how types used in your code will be mapped / encoded in the generated Avro schemas and files.
If a type can be mapped in multiple ways, it is listed more than once.

| Scala Type                   	         | Schema Type   	    | Logical Type     	| Encoded Type                      |
|----------------------------------------|--------------------|------------------	|-----------------------------------|
| String                       	         | STRING        	    |                  	| Utf8                              |
| String                       	         | FIXED        	     |                  	| GenericFixed                      |
| String                       	         | BYTES        	     |                  	| ByteBuffer                        |
| Boolean                      	         | BOOLEAN       	    |                  	| java.lang.Boolean                 |
| Long                         	         | LONG          	    |                  	| java.lang.Long                    |
| Int                          	         | INT           	    |                  	| java.lang.Integer                 |
| Short                        	         | INT           	    |                  	| java.lang.Integer                 |
| Byte                         	         | INT           	    |                  	| java.lang.Integer                 |
| Double                       	         | DOUBLE        	    |                  	| java.lang.Double                  |
| Float                        	         | FLOAT         	    |                  	| java.lang.Float                   |
| UUID                         	         | STRING        	    | UUID             	| Utf8                              |
| LocalDate                    	         | INT           	    | Date             	| java.lang.Int                     |
| LocalTime                    	         | INT           	    | time-millis      	| java.lang.Int                     |
| LocalDateTime                	         | LONG          	    | timestamp-nanos 	| java.lang.Long                    |
| java.sql.Date                	         | INT           	    | Date             	| java.lang.Int                     |
| Instant                      	         | LONG          	    | Timestamp-Millis 	| java.lang.Long                    |
| Timestamp                    	         | LONG          	    | Timestamp-Millis 	| java.lang.Long                    |
| BigDecimal                   	         | BYTES         	    | Decimal<8,2>     	| ByteBuffer                        |
| BigDecimal                   	         | FIXED         	    | Decimal<8,2>     	| GenericFixed                      |
| BigDecimal                   	         | STRING         	   | Decimal<8,2>     	| String                            |
| Option[T]                    	         | UNION<null,T> 	    |                  	| null, T                           |
| Array[Byte]                  	         | BYTES         	    |                  	| ByteBuffer                        |
| Array[Byte]                  	         | FIXED         	    |                  	| GenericFixed                      |
| ByteBuffer                   	         | BYTES         	    |                  	| ByteBuffer                        |
| Seq[Byte]                    	         | BYTES         	    |                  	| ByteBuffer                        |
| List[Byte]                   	         | BYTES         	    |                  	| ByteBuffer                        |
| Vector[Byte]                 	         | BYTES         	    |                  	| ByteBuffer                        |
| Array[T]                     	         | ARRAY<T>      	    |                  	| Array[T]                          |
| Vector[T]                    	         | ARRAY<T>      	    |                  	| Array[T]                          |
| Seq[T]                       	         | ARRAY<T>      	    |                  	| Array[T]                          |
| List[T]                      	         | ARRAY<T>      	    |                  	| Array[T]                          |
| Set[T]                       	         | ARRAY<T>      	    |                  	| Array[T]                          |
| sealed trait of case classes 	         | UNION<A,B,..>  	   |                  	| A, B, ...                         |
| sealed trait of case objects 	         | ENUM<A,B,..>  	    |                  	| GenericEnumSymbol                 |
| Map[String, V]              	          | MAP<V>        	    |                  	| java.util.Map[String, V]          |
| Either[A,B]                  	         | UNION<A,B>    	    |                  	| A, B                              |
| A :+: B :+: C :+: CNil       	         | UNION<A,B,C>  	    |                  	| A, B, ...                         |
| case class T                 	         | RECORD        	    |                  	| GenericRecord with SpecificRecord |
| Scala enumeration            	         | ENUM          	    |                  	| GenericEnumSymbol                 |
| Java enumeration             	         | ENUM          	    |                  	| GenericEnumSymbol                 |
| Scala tuples                           | RECORD             |                   | GenericRecord with SpecificRecord |
| Option[Either[A,B]]                    | UNION<null,A,B>    |                   | null, A, B                        |
| option of sealed trait of case classes | UNION<null,A,B,..> |                   | null, A, B, ...                   |
| option of sealed trait of case objects   | UNION<null,A,B,..> |                   | null, GenericEnumSymbol           |

To select the encoding in case multiple encoded types exist, create a new `Encoder` with a corresponding `SchemaFor` 
instance to the via `withSchema`. For example, creating a string encoder that uses target type `BYTES` works like this:

```scala
val stringSchemaFor = SchemaFor[String](Schema.create(Schema.Type.BYTES))
val stringEncoder = Encoder[String].withSchema(stringSchemaFor)
``` 

### Custom Type Mappings

It is very easy to add custom type mappings. To do this, we bring into scope a custom implicit of `Encoder[T]` and/or `Decoder[T]`.

For example, to create a custom type mapping for a type Foo which writes out the contents in upper case, but always reads
the contents in lower case, we can do the following:

```scala
case class Foo(a: String, b: String)

implicit object FooEncoder extends Encoder[Foo] {

  override val schemaFor = SchemaFor[Foo]

  override def encode(foo: Foo) = {
    val record = new GenericData.Record(schema)
    record.put("a", foo.a.toUpperCase)
    record.put("b", foo.b.toUpperCase)
    record
  }
}

implicit object FooDecoder extends Decoder[Foo] {

  override val schemaFor = SchemaFor[Foo]

  override def decode(value: Any) = {
    val record = value.asInstanceOf[GenericRecord]
    Foo(record.get("a").toString.toLowerCase, record.get("b").toString.toLowerCase)
  }
}
```

Another example is changing the way we serialize `LocalDateTime` to store these dates as ISO strings. In this case, we are
writing out a String rather than the default Long so we must also change the schema type. Therefore, we must add an implicit `SchemaFor` as well as the encoders
and decoders.

```scala
implicit val LocalDateTimeSchemaFor = SchemaFor[LocalDateTime](Schema.create(Schema.Type.STRING))

implicit object DateTimeEncoder extends Encoder[LocalDateTime] {

  override val schemaFor = LocalDateTimeSchemaFor

  override def encode(value: LocalDateTime) = 
    ISODateTimeFormat.dateTime().print(value)
}

implicit object DateTimeDecoder extends Decoder[LocalDateTime] {

  override val schemaFor = LocalDateTimeSchemaFor

  override def decode(value: Any) = 
    ISODateTimeFormat.dateTime().parseDateTime(value.toString)
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

In order to customize the scale and precision used by `BigDecimal` schema generators, bring an implicit `ScalePrecision` instance into scope.before using `AvroSchema`.

```scala
import com.sksamuel.avro4s.ScalePrecision

case class MyDecimal(d: BigDecimal)

implicit val sp = ScalePrecision(4, 20)
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
      "scale":"4",
      "precision":"20"
    }
  }]
}
```

When encoding values, it may be necessary to round values if they need to be converted to the scale used by the schema. By default this is `RoundingMode.UNNECESSARY` which will throw an exception if rounding is required.
In order to change this, add an implicit `RoundingMode` value before the `Encoder` is generated.

```scala
case class MyDecimal(d: BigDecimal)

implicit val sp = ScalePrecision(4, 20)
val schema = AvroSchema[MyDecimal]

implicit val roundingMode = RoundingMode.HALF_UP
val encoder = Encoder[MyDecimal]
``` 

## Type Parameters

When serializing a class with one or more type parameters, the avro name used in a schema is the name of the raw type, plus the actual type parameters. In other words, it would be of the form `rawtype__typeparam1_typeparam2_..._typeparamN`. So for example, the schema for a type `Event[Foo]` would have the avro name `event__foo`.

You can disable this by annotating the class with `@AvroErasedName` which uses the JVM erased name - in other words, it drops type parameter information. So the aforementioned `Event[Foo]` would be simply `event`.

## Selective Customisation

You can selectively customise the way Avro4s generates certain parts of your hierarchy, thanks to implicit precedence. Suppose you have the following classes:

```scala
case class Product(name: String, price: Price, litres: BigDecimal)
case class Price(currency: String, amount: BigDecimal)
```

And you want to selectively use different scale/precision for the `price` and `litres` quantities. You can do this by forcing the implicits in the corresponding companion objects.

``` scala
object Price {
  implicit val sp = ScalePrecision(10, 2)
  implicit val schema = SchemaFor[Price]
}

object Product {
  implicit val sp = ScalePrecision(8, 4)
  implicit val schema = SchemaFor[Product]
}
```

This will result in a schema where both `BigDecimal` quantities have their own separate scale and precision.

## Cats Support

If you use cats in your domain objects, then Avro4s provides a cats module with schemas, encoders and decoders for some cats types.
Just import `import com.sksamuel.avro4s.cats._` before calling into the macros.

```scala
case class Foo(list: NonEmptyList[String], vector: NonEmptyVector[Boolean])
val schema = AvroSchema[Foo]
```

## Refined Support

If you use [refined](https://github.com/fthomas/refined) in your domain objects, then Avro4s provides a refined module with schemas, encoders and decoders for refined types.
Just import `import com.sksamuel.avro4s.refined._` before calling into the macros.

```scala
case class Foo(nonEmptyStr: String Refined NonEmpty)
val schema = AvroSchema[Foo]
```

### Mapping Recursive Types

Avro4s supports encoders and decoders for recursive types. Customizing them is possible, but involved. As with customizing
SchemaFor instances for recursive types, the simplest way to customize encoders and decoders is to provide a custom
encoder and decoder for all types that form the recursion.

If that isn't possible, you can customize encoders / decoders for one single type and participate in creating a cyclic
graph of encoders / decoders. To give an example, consider the following recursive type for trees.

```scala
sealed trait Tree[+T]
case class Branch[+T](left: Tree[T], right: Tree[T]) extends Tree[T]
case class Leaf[+T](value: T) extends Tree[T]
```

For this, a custom `Branch[Int]` encoder can be defined as follows.

```scala
// 1. use implicit def so that Encoder.apply[Tree[Int]] in step 7. and 10. below picks this resolvable encoder for branches.
// 2. implement a ResolvableEncoder instead of Encoder directly so that encoder creation can be deferred
implicit def branchEncoder: Encoder[Branch[Int]] = new ResolvableEncoder[Branch[Int]] {

def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[Branch[Int]] =
  // 3. lookup in the definition environment whether we already have created an encoder for branch.
  env.get[Branch[Int]].getOrElse {

    // 4. use var here to first create an acyclic graph and close it later.
    var treeEncoder: Encoder[Tree[Int]] = null

    // 5. create a partially initialized encoder for branches (it lacks a value for treeEncoder on creation).
    val encoder = new Encoder[Branch[Int]] {
      val schemaFor: SchemaFor[Branch[Int]] = SchemaFor[Branch[Int]]

      def encode(value: Branch[Int]): AnyRef =
        ImmutableRecord(schema, Seq(treeEncoder.encode(value.left), treeEncoder.encode(value.right)))
    }

    // 6. extend the definition environment with the newly created encoder so that subsequent lookups (step 3.) can return it
    val nextEnv = env.updated(encoder)

    // 7. resolve the tree encoder with the extended environment; the extended env will be passed back to the lookup
    //    performed in step 3. above.
    // 9. complete the initialization by closing the reference cycle: the branch encoder and tree encoder now 
    //    reference each other.
    treeEncoder = Encoder.apply[Tree[Int]].resolveEncoder(nextEnv, NoUpdate)
    encoder
  }
}

// 10. summon encoder for tree and kick off encoder resolution.
val toRecord = ToRecord[Tree[Int]]
```

Why is this so complicated? Glad you asked! Turns out that caring for performance, providing customization via 
annotations, and using Magnolia for automatic typeclass derivation (which is great in itself) are three constraints 
that aren't easy to combine. This design is the best we came up with; if you have a better design for this, please 
contribute it!

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
