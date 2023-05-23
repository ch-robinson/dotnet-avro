Chr.Avro is designed to work without requiring any advanced setup or customization. However, it’s possible to customize most of Chr.Avro’s default behaviors by adding, removing, or rearranging cases.

## Mapping an interface to concrete types

Suppose you have a schema that contains a union of records:

```json
{
  "name": "example.OrderEventRecord",
  "type": "record",
  "fields": [{
    "name": "timestamp",
    "type": {
      "type": "int",
      "logicalType": "timestamp-millis"
    }
  }, {
    "name": "event",
    "type": [{
      "name": "example.OrderCreationEvent",
      "type": "record",
      "fields": [{
        "name": "lineItems",
        "type": {
          "type": "array",
          "items": {
            "name": "example.OrderLineItem",
            "type": "record",
            "fields": [{
              "name": "productId",
              "type": {
                "type": "string",
                "logicalType": "uuid"
              }
            }, {
              "name": "quantity",
              "type": "int"
            }]
          }
        }
      }]
    }, {
      "name": "example.OrderLineItemModificationEvent",
      "type": "record",
      "fields": [{
        "name": "index",
        "type": "int"
      }, {
        "name": "lineItem",
        "type": "OrderLineItem"
      }]
    }, {
      "name": "example.OrderCancellationEvent",
      "type": "record",
      "fields": []
    }]
  }]
}
```

A matching class hierarchy might look like this:

```csharp
using System;
using System.Collections.Generic;

public class OrderEventRecord
{
    public IOrderEvent Event { get; set; }
    public DateTime Timestamp { get; set; }
}

public interface IOrderEvent
{
}

public class OrderCreationEvent : IOrderEvent
{
    public IList<OrderLineItem> LineItems { get; set; }
}

public class OrderLineItemModificationEvent : IOrderEvent
{
    public int Index { get; set; }
    public OrderLineItem LineItem { get; set; }
}

public class OrderCancellationEvent : IOrderEvent
{
}

public class OrderLineItem
{
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
}
```

Out of the box, Chr.Avro won’t be able to figure out this mapping. When building a serializer, it will try to map `IOrderEvent` to each schema in the union and fail because there are multiple matches. When building a deserializer, it will fail because `IOrderEvent` is not a concrete type.

To support this type of advanced mapping, applications can provide custom cases for the serializer and deserializer builders. The cases will match the union schema and the `IOrderEvent` interface and choose the appropriate concrete class:

```csharp
using System;
using System.Linq.Expressions;
using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;

public class OrderEventUnionDeserializerBuilderCase : BinaryUnionDeserializerBuilderCase
{
    public CustomUnionDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
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
                new UnsupportedTypeException(type, $"{nameof(CustomUnionDeserializerBuilderCase)} can only be applied to the {typeof(IOrderEvent)} type."));
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

public class OrderEventUnionSerializerBuilderCase : BinaryUnionSerializerBuilderCase
{
    public OrderEventUnionSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        : base(serializerBuilder)
    {
    }

    public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
    {
        if (type == typeof(IOrderEvent))
        {
            return base.BuildExpression(value, type, schema, context);
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
```

In this example, the custom cases rely on record schema names to pick the correct concrete class. Other strategies work too—partial or fuzzy name matching, matching based on record fields, or even relying on custom schema metadata.

Custom cases should generally be prepended to the default cases to ensure that they take precedence:

```csharp
using System.Linq;
using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using Chr.Avro.Serialization;

BinaryDeserializer<OrderEventRecord> CreateEventDeserializer(Schema schema)
{
    return new BinaryDeserializerBuilder(
        BinaryDeserializerBuilder.CreateDefaultCaseBuilders()
            .Prepend(builder => new OrderEventUnionDeserializerBuilderCase(builder)))
        .BuildDelegate<EventRecord>(schema);
}

BinarySerializer<OrderEventRecord> CreateEventSerializer(Schema schema)
{
    return new BinarySerializerBuilder(
        BinarySerializerBuilder.CreateDefaultCaseBuilders()
            .Prepend(builder => new OrderEventUnionSerializerBuilderCase(builder)))
        .BuildDelegate<EventRecord>(schema);
}
```

With those custom cases in place, Chr.Avro will be able to properly serialize and deserialize `OrderEventRecord`s. For a working example, see the [Chr.Avro.UnionTypeExample](https://github.com/ch-robinson/dotnet-avro/tree/main/examples/Chr.Avro.UnionTypeExample) project.
