namespace Chr.Avro.Fixtures
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    public class PolymorphicClassAUnionSerializerBuilderCase : BinaryUnionSerializerBuilderCase
    {
        public PolymorphicClassAUnionSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
            : base(serializerBuilder)
        {
        }

        public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (type == typeof(PolymorphicClassA))
            {
                return base.BuildExpression(value, type, schema, context);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(PolymorphicClassAUnionSerializerBuilderCase)} can only be applied to the {typeof(PolymorphicClassA)} type."));
            }
        }

        protected override Type SelectType(Type type, Schema schema)
        {
            return (schema as RecordSchema)?.Name switch
            {
                nameof(PolymorphicClassAA) => typeof(PolymorphicClassAA),
                nameof(PolymorphicClassAB) => typeof(PolymorphicClassAB),
                _ => throw new UnsupportedSchemaException(schema),
            };
        }
    }
}
