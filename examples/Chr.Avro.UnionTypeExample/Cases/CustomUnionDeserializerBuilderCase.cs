namespace Chr.Avro.UnionTypeExample.Cases
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using Chr.Avro.UnionTypeExample.Models;

    public class CustomUnionDeserializerBuilderCase : BinaryUnionDeserializerBuilderCase
    {
        public CustomUnionDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
            : base(deserializerBuilder)
        {
        }

        public override BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (type == typeof(IDataObj))
            {
                return base.BuildExpression(type, schema, context);
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(CustomUnionDeserializerBuilderCase)} can only be applied to the {typeof(IDataObj)} type."));
            }
        }

        protected override Type SelectType(Type type, Schema schema)
        {
            return (schema as RecordSchema)?.Name switch
            {
                nameof(DataObj1) => typeof(DataObj1),
                nameof(DataObj2) => typeof(DataObj2),
                nameof(DataObj3) => typeof(DataObj3),
                _ => throw new UnsupportedSchemaException(schema),
            };
        }
    }
}
