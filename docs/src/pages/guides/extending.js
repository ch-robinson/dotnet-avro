import { graphql, useStaticQuery } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'

const title = 'Extending and overriding built-in features'

export default () => {
  const {
    site: {
      siteMetadata: { projectName }
    }
  } = useStaticQuery(graphql`
    query {
      site {
        siteMetadata {
          projectName
        }
      }
    }
  `)

  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>

      <h1>{title}</h1>
      <p>{projectName} is designed to work without requiring any advanced setup or customization. However, it’s possible to customize most of {projectName}’s default behaviors by adding, removing, or rearranging cases.</p>

      <h2>Mapping an interface to concrete types</h2>
      <p>Suppose you have a schema that contains a union of records:</p>
      <Highlight language='avro'>{`{
  "name": "example.EventRecord",
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
      "name": "example.order.OrderCreation",
      "type": "record",
      "fields": [{
        "name": "lineItems",
        "type": {
          "type": "array",
          "items": {
            "name": "example.order.OrderLineItem",
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
      "name": "example.order.OrderLineItemModification",
      "type": "record",
      "fields": [{
        "name": "index",
        "type": "int"
      }, {
        "name": "lineItem",
        "type": "OrderLineItem"
      }]
    }, {
      "name": "example.order.OrderCancellation",
      "type": "record",
      "fields": []
    }]
  }]
}`}</Highlight>
      <p>A matching class hierarchy might look like this:</p>
      <Highlight language='csharp'>{`using System;
using System.Collections.Generic;

public class EventRecord
{
    public IEvent Event { get; set; }
    public DateTime Timestamp { get; set; }
}

public interface IEvent
{

}

public class OrderCreation : IEvent
{
    public IEnumerable<OrderLineItem> LineItems { get; set; }
}

public class OrderLineItemModification : IEvent
{
    public int Index { get; set; }
    public OrderLineItem LineItem { get; set; }
}

public class OrderCancellation : IEvent
{

}

public class OrderLineItem
{
    Guid ProductId { get; set; }
    int Quantity { get; set; }
}`}</Highlight>
      <p>Out of the box, {projectName} won’t be able to figure out this mapping. When building a serializer, it will try to map <code>IEvent</code> to each schema in the union and fail because there are multiple matches. When building a deserializer, it will fail because <code>IEvent</code> is not a concrete type.</p>
      <p>To support this type of advanced mapping, applications can provide custom cases for the serializer and deserializer builders. The cases will match the union schema and the <code>IEvent</code> interface and choose the appropriate concrete class:</p>
      <Highlight language='csharp'>{`using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using Chr.Avro.Serialization;

public class OrderDeserializerBuilderCase : UnionDeserializerBuilderCase
{
    public ITypeResolver Resolver { get; }

    public OrderDeserializerBuilderCase(ITypeResolver resolver, IBinaryCodec codec, IBinaryDeserializerBuilder builder) : base(codec, builder)
    {
        Resolver = resolver;
    }

    protected override TypeResolution SelectType(TypeResolution resolution, Schema schema)
    {
        if (!(resolution is RecordResolution recordResolution) || recordResolution.Type != typeof(IEvent))
        {
            throw new UnsupportedTypeException(resolution.Type);
        }

        switch ((schema as RecordSchema)?.Name)
        {
            case nameof(OrderCancellation):
                return Resolver.ResolveType<OrderCancellation>();

            case nameof(OrderCreation):
                return Resolver.ResolveType<OrderCreation>();

            case nameof(OrderLineItemModification):
                return Resolver.ResolveType<OrderLineItemModification>();

            default:
                throw new UnsupportedSchemaException(schema);
        }
    }
}

public class OrderSerializerBuilderCase : UnionSerializerBuilderCase
{
    public ITypeResolver Resolver { get; }

    public OrderSerializerBuilderCase(ITypeResolver resolver, IBinaryCodec codec, IBinarySerializerBuilder builder) : base(codec, builder)
    {
        Resolver = resolver;
    }

    protected override TypeResolution SelectType(TypeResolution resolution, Schema schema)
    {
        if (!(resolution is RecordResolution recordResolution) || recordResolution.Type != typeof(IEvent))
        {
            throw new UnsupportedTypeException(resolution.Type);
        }

        switch ((schema as RecordSchema)?.Name)
        {
            case nameof(OrderCancellation):
                return Resolver.ResolveType<OrderCancellation>();

            case nameof(OrderCreation):
                return Resolver.ResolveType<OrderCreation>();

            case nameof(OrderLineItemModification):
                return Resolver.ResolveType<OrderLineItemModification>();

            default:
                throw new UnsupportedSchemaException(schema);
        }
    }
}`}</Highlight>
      <p>In this example, the custom cases rely on record schema names to pick the correct concrete class. Other strategies work too—partial or fuzzy name matching, matching based on record fields, or even relying on custom schema metadata.</p>
      <p>Custom cases should generally be prepended to the default cases to ensure that they take precedence:</p>
      <Highlight language='csharp'>{`using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using Chr.Avro.Serialization;

IBinaryDeserializer<EventRecord> CreateEventDeserializer(Schema schema)
{
    var codec = new BinaryCodec();
    var resolver = new ReflectionResolver();

    return new BinaryDeserializerBuilder(BinaryDeserializerBuilder.CreateBinaryDeserializerCaseBuilders(codec)
        .Prepend(builder => new OrderDeserializerBuilderCase(resolver, codec, builder)))
        .BuildDeserializer<EventRecord>(schema);
}

IBinarySerializer<EventRecord> CreateEventSerializer(Schema schema)
{
    var codec = new BinaryCodec();
    var resolver = new ReflectionResolver();

    return new BinarySerializerBuilder(BinarySerializerBuilder.CreateBinarySerializerCaseBuilders(codec)
        .Prepend(builder => new OrderSerializerBuilderCase(resolver, codec, builder)))
        .BuildSerializer<EventRecord>(schema);
}`}</Highlight>
      <p>With those custom cases in place, {projectName} will be able to properly serialize and deserialize <code>EventRecord</code>s.</p>
    </>
  )
}
