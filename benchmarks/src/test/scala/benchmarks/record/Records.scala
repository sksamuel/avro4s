package benchmarks.record

case class RecordWithSimpleField(attribute: IntAttributeValue.Valid)

case class RecordWithUnionField(attribute: IntAttributeValue)

case class RecordWithTypeParamField(attribute: AttributeValue.Valid[Int])

case class RecordWithUnionAndTypeField(attribute: AttributeValue[Int])
