namespace Chr.Avro.Fixtures
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    public class PolymorphicClassAUnionDeserializerBuilderCase : BinaryUnionDeserializerBuilderCase
    {
        public PolymorphicClassAUnionDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
            : base(deserializerBuilder)
        {
        }

        public override BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (type == typeof(PolymorphicClassA))
            {
                return base.BuildExpression(type, schema, context);
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(PolymorphicClassBUnionDeserializerBuilderCase)} can only be applied to the {typeof(PolymorphicClassA)} type."));
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
