namespace Chr.Avro.Fixtures
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    public class PolymorphicClassBUnionDeserializerBuilderCase : BinaryUnionDeserializerBuilderCase
    {
        public PolymorphicClassBUnionDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
            : base(deserializerBuilder)
        {
        }

        public override BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (type == typeof(PolymorphicClassB))
            {
                return base.BuildExpression(type, schema, context);
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(PolymorphicClassBUnionDeserializerBuilderCase)} can only be applied to the {typeof(PolymorphicClassB)} type."));
            }
        }

        protected override Type SelectType(Type type, Schema schema)
        {
            return (schema as RecordSchema)?.Name switch
            {
                nameof(PolymorphicClassBA) => typeof(PolymorphicClassBA),
                nameof(PolymorphicClassBB) => typeof(PolymorphicClassBB),
                _ => throw new UnsupportedSchemaException(schema),
            };
        }
    }
}
