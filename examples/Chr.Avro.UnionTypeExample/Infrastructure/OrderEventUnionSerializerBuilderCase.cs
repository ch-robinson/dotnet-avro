namespace Chr.Avro.UnionTypeExample.Infrastructure
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using Chr.Avro.UnionTypeExample.Models;

    public class OrderEventUnionSerializerBuilderCase : BinaryUnionSerializerBuilderCase
    {
        public OrderEventUnionSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
            : base(serializerBuilder)
        {
        }

        public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (type == typeof(IOrderEvent))
            {
                return base.BuildExpression(value, type, schema, context, registerExpression);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(OrderEventUnionSerializerBuilderCase)} can only be applied to the {typeof(IOrderEvent)} type."));
            }
        }

        protected override Type SelectType(Type type, Schema schema)
        {
            return (schema as RecordSchema)?.Name switch
            {
                nameof(OrderCreationEvent) => typeof(OrderCreationEvent),
                nameof(OrderLineItemModificationEvent) => typeof(OrderLineItemModificationEvent),
                nameof(OrderCancellationEvent) => typeof(OrderCancellationEvent),
                _ => throw new UnsupportedSchemaException(schema),
            };
        }
    }
}
