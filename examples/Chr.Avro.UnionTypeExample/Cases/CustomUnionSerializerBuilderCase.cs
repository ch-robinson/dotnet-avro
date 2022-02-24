namespace Chr.Avro.UnionTypeExample.Cases
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using Chr.Avro.UnionTypeExample.Models;

    public class CustomUnionSerializerBuilderCase : BinaryUnionSerializerBuilderCase
    {
        public CustomUnionSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
            : base(serializerBuilder)
        {
        }

        public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (type == typeof(IDataObj))
            {
                return base.BuildExpression(value, type, schema, context);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(CustomUnionSerializerBuilderCase)} can only be applied to the {typeof(IDataObj)} type."));
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
