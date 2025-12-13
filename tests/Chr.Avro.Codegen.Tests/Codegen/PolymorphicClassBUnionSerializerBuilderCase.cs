namespace Chr.Avro.Fixtures
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    public class PolymorphicClassBUnionSerializerBuilderCase : BinaryUnionSerializerBuilderCase
    {
        public PolymorphicClassBUnionSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
            : base(serializerBuilder)
        {
        }

        public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (type == typeof(PolymorphicClassB))
            {
                return base.BuildExpression(value, type, schema, context);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(PolymorphicClassBUnionSerializerBuilderCase)} can only be applied to the {typeof(PolymorphicClassB)} type."));
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
