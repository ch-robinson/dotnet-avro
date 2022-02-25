namespace Chr.Avro.UnionTypeExample.Infrastructure
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.UnionTypeExample.Models;

    public class OrderEventUnionSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        private readonly ISchemaBuilder schemaBuilder;

        public OrderEventUnionSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            this.schemaBuilder = schemaBuilder;
        }

        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(IOrderEvent))
            {
                return SchemaBuilderCaseResult.FromSchema(
                    new UnionSchema(new[]
                    {
                        schemaBuilder.BuildSchema<OrderCreationEvent>(context),
                        schemaBuilder.BuildSchema<OrderLineItemModificationEvent>(context),
                        schemaBuilder.BuildSchema<OrderCancellationEvent>(context),
                    }));
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(OrderEventUnionSchemaBuilderCase)} can only be applied to the {typeof(IOrderEvent)} type."));
            }
        }
    }
}
