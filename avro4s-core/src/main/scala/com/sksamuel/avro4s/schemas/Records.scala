package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaConfiguration
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.mutable.WeakHashMap
import scala.jdk.CollectionConverters._

object RecordCache {

  private val map: WeakHashMap[String, Any] = WeakHashMap.empty[String, Any]

  /**
   * Extend the environment with a definition for type `T` - uses a `WeakTypeTag` as entry key.
   */
  def update[T](fqn: String, typeclass: Any) = {
    println(s"Setting $fqn = $typeclass")
    map.addOne(fqn, typeclass)
  }

  /**
   * Retrieve an already existing definition definition, given a `WeakTypeTag` as key.
   */
  def schemaFor[T](fqn: String): Option[SchemaFor[T]] = map.get(fqn).asInstanceOf[Option[SchemaFor[T]]]
}

object Records {

  def record[T](name: String, doc: Option[String], namespace: String, isError: Boolean, tofields: Seq[ToField]): SchemaFor[T] = {

    // when we resolve the fields, some nested field may require a schema for the same type T as we are building here.
    // this would lead to infinite loops so we need to know when we have already seen a type, and just reference that
    // type without trying to define it again.
    new SchemaFor[T] {
      override def schema(config: SchemaConfiguration): Schema = {
        val fqn = s"$namespace.$name"
        val newConfig = config.withEnv(config.env.add(fqn))
        val fields = tofields.map(_.field(newConfig))
        Schema.createRecord(name, doc.orNull, namespace, isError, fields.asJava)
      }
    }
  }

  def field[F](name: String, doc: Option[String], namespace: String, schemaFor: SchemaFor[F]): ToField = new ToField {
    override def field(config: SchemaConfiguration): Schema.Field = {
      val n = config.mapper.to(name)
      val schema = if (config.env.contains(s"$namespace.$name")) SchemaBuilder.builder().`type`(name, namespace) else schemaFor.schema(config)
      new Schema.Field(n, schema, doc.orNull)
    }
  }
}

trait ToField {
  def field(config: SchemaConfiguration): Schema.Field
}

private[this] class SettableSchemaFor[T] extends SchemaFor[T] {
  private var schema: Schema = null
  def set(schema: Schema) = this.schema = schema
  override def schema(config: SchemaConfiguration): Schema = schema
}
