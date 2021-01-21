using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class UnionSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public UnionSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Fact]
        public void EmptyUnionToObjectType()
        {
            var schema = new UnionSchema();

            Assert.Throws<UnsupportedSchemaException>(() => _deserializerBuilder.BuildDelegate<object>(schema));
            Assert.Throws<UnsupportedSchemaException>(() => _serializerBuilder.BuildDelegate<object>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntValues))]
        public void NullAndIntUnionToInt32Type(int? value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            var serialize = _serializerBuilder.BuildDelegate<int>(schema);

            if (value.HasValue)
            {
                using (_stream)
                {
                    serialize(value.Value, new Utf8JsonWriter(_stream));
                }
            }

            Assert.Throws<UnsupportedTypeException>(() => _deserializerBuilder.BuildDelegate<int>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntValues))]
        public void NullAndIntUnionToNullableInt32Type(int? value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            var deserialize = _deserializerBuilder.BuildDelegate<int?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int?>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void NullAndIntUnionToStringType()
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            Assert.Throws<UnsupportedTypeException>(() => _serializerBuilder.BuildDelegate<string>(schema));
            Assert.Throws<UnsupportedTypeException>(() => _deserializerBuilder.BuildDelegate<string>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndStringValues))]
        public void NullAndStringUnionToStringType(string value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new StringSchema()
            });

            var deserialize = _deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(NullValues))]
        public void NullUnionToStringType(string value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema()
            });

            var deserialize = _deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void PartiallySelectedTypes()
        {
            var schema = new UnionSchema(new[]
            {
                new RecordSchema(nameof(OrderCancelledEvent), new[]
                {
                    new RecordField("timestamp", new StringSchema())
                }),
                new RecordSchema(nameof(OrderCreatedEvent), new[]
                {
                    new RecordField("timestamp", new StringSchema()),
                    new RecordField("total", new BytesSchema()
                    {
                        LogicalType = new DecimalLogicalType(5, 2)
                    })
                })
            });

            var resolver = new ReflectionResolver();

            var deserialize = _deserializerBuilder.BuildDelegate<OrderCreatedEvent>(schema);

            var serialize = new JsonSerializerBuilder(JsonSerializerBuilder.DefaultCaseBuilders
                .Prepend(builder => new OrderSerializerBuilderCase(resolver, builder)))
                .BuildDelegate<OrderEvent>(schema);

            var value = new OrderCreatedEvent
            {
                Timestamp = DateTime.UtcNow,
                Total = 40M
            };

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            var result = deserialize(ref reader);
            Assert.Equal(value.Timestamp, result.Timestamp);
            Assert.Equal(value.Total, result.Total);
        }

        [Fact]
        public void SelectedTypes()
        {
            var schema = new RecordSchema(nameof(EventContainer), new[]
            {
                new RecordField("event", new UnionSchema(new[]
                {
                    new RecordSchema(nameof(OrderCreatedEvent), new[]
                    {
                        new RecordField("timestamp", new StringSchema()),
                        new RecordField("total", new BytesSchema()
                        {
                            LogicalType = new DecimalLogicalType(5, 2)
                        })
                    }),
                    new RecordSchema(nameof(OrderCancelledEvent), new[]
                    {
                        new RecordField("timestamp", new StringSchema())
                    })
                }))
            });

            var resolver = new ReflectionResolver();

            var deserialize = new JsonDeserializerBuilder(JsonDeserializerBuilder.DefaultCaseBuilders
                .Prepend(builder => new OrderDeserializerBuilderCase(resolver, builder)))
                .BuildDelegate<EventContainer>(schema);

            var serialize = new JsonSerializerBuilder(JsonSerializerBuilder.DefaultCaseBuilders
                .Prepend(builder => new OrderSerializerBuilderCase(resolver, builder)))
                .BuildDelegate<EventContainer>(schema);

            var creation = new EventContainer
            {
                Event = new OrderCreatedEvent
                {
                    Timestamp = DateTime.UtcNow,
                    Total = 40M
                }
            };

            var cancellation = new EventContainer
            {
                Event = new OrderCancelledEvent
                {
                    Timestamp = DateTime.UtcNow
                }
            };

            using (_stream)
            {
                serialize(creation, new Utf8JsonWriter(_stream));

                var reader = new Utf8JsonReader(_stream.ToArray());

                var result = deserialize(ref reader);
                Assert.IsType<OrderCreatedEvent>(result.Event);

                _stream.Position = 0;
                serialize(cancellation, new Utf8JsonWriter(_stream));

                reader = new Utf8JsonReader(_stream.ToArray());

                result = deserialize(ref reader);
                Assert.IsType<OrderCancelledEvent>(result.Event);
            }
        }

        [Theory]
        [MemberData(nameof(StringValues))]
        public void StringUnionToStringType(string value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new StringSchema()
            });

            var deserialize = _deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        public static IEnumerable<object[]> NullAndIntValues => new List<object[]>
        {
            new object[] { null },
            new object[] { 2 },
        };

        public static IEnumerable<object[]> NullAndStringValues => new List<object[]>
        {
            new object[] { null },
            new object[] { "test" },
        };

        public static IEnumerable<object[]> NullValues => new List<object[]>
        {
            new object[] { null },
        };

        public static IEnumerable<object[]> StringValues => new List<object[]>
        {
            new object[] { "test" },
        };

        public class EventContainer
        {
            public IEvent Event { get; set; }
        }

        public interface IEvent
        {
            DateTime Timestamp { get; }
        }

        public abstract class OrderEvent : IEvent
        {
            public DateTime Timestamp { get; set; }
        }

        public class OrderCreatedEvent : OrderEvent
        {
            public decimal Total { get; set; }
        }

        public class OrderCancelledEvent : OrderEvent { }

        public class OrderDeserializerBuilderCase : UnionDeserializerBuilderCase
        {
            public ITypeResolver Resolver { get; }

            public OrderDeserializerBuilderCase(ITypeResolver resolver, IJsonDeserializerBuilder builder) : base(builder)
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
                    case nameof(OrderCreatedEvent):
                        return Resolver.ResolveType<OrderCreatedEvent>();

                    case nameof(OrderCancelledEvent):
                        return Resolver.ResolveType<OrderCancelledEvent>();

                    default:
                        throw new UnsupportedSchemaException(schema);
                }
            }
        }

        public class OrderSerializerBuilderCase : UnionSerializerBuilderCase
        {
            public ITypeResolver Resolver { get; }

            public OrderSerializerBuilderCase(ITypeResolver resolver, IJsonSerializerBuilder builder) : base(builder)
            {
                Resolver = resolver;
            }

            protected override TypeResolution SelectType(TypeResolution resolution, Schema schema)
            {
                if (!(resolution is RecordResolution recordResolution) || !recordResolution.Type.IsAssignableFrom(typeof(OrderEvent)))
                {
                    throw new UnsupportedTypeException(resolution.Type);
                }

                switch ((schema as RecordSchema)?.Name)
                {
                    case nameof(OrderCreatedEvent):
                        return Resolver.ResolveType<OrderCreatedEvent>();

                    case nameof(OrderCancelledEvent):
                        return Resolver.ResolveType<OrderCancelledEvent>();

                    default:
                        throw new UnsupportedSchemaException(schema);
                }
            }
        }
    }
}
