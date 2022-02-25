namespace Chr.Avro.UnionTypeExample.Infrastructure
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using Chr.Avro.UnionTypeExample.Models;

    public class OrderEventUnionDeserializerBuilderCase : BinaryUnionDeserializerBuilderCase
    {
        public OrderEventUnionDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
            : base(deserializerBuilder)
        {
        }

        public override BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (type == typeof(IOrderEvent))
            {
                return base.BuildExpression(type, schema, context);
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(OrderEventUnionDeserializerBuilderCase)} can only be applied to the {typeof(IOrderEvent)} type."));
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
