namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class UnionSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public UnionSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public interface IEvent
        {
            DateTime Timestamp { get; }
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

        [Fact]
        public void EmptyUnionToObjectType()
        {
            var schema = new UnionSchema();

            Assert.Throws<UnsupportedSchemaException>(() => deserializerBuilder.BuildDelegate<object>(schema));
            Assert.Throws<UnsupportedSchemaException>(() => serializerBuilder.BuildDelegate<object>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntValues))]
        public void NullAndIntUnionToInt32Type(int? value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema(),
            });

            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            if (value.HasValue)
            {
                using (stream)
                {
                    serialize(value.Value, new Utf8JsonWriter(stream));
                }
            }

            Assert.Throws<UnsupportedTypeException>(() => deserializerBuilder.BuildDelegate<int>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntValues))]
        public void NullAndIntUnionToNullableInt32Type(int? value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema(),
            });

            var deserialize = deserializerBuilder.BuildDelegate<int?>(schema);
            var serialize = serializerBuilder.BuildDelegate<int?>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void NullAndIntUnionToStringType()
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema(),
            });

            Assert.Throws<UnsupportedTypeException>(() => serializerBuilder.BuildDelegate<string>(schema));
            Assert.Throws<UnsupportedTypeException>(() => deserializerBuilder.BuildDelegate<string>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndStringValues))]
        public void NullAndStringUnionToStringType(string value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new StringSchema(),
            });

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(NullValues))]
        public void NullUnionToStringType(string value)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
            });

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void PartiallySelectedTypes()
        {
            var schema = new UnionSchema(new[]
            {
                new RecordSchema(nameof(OrderCancelledEvent))
                {
                    Fields = new[]
                    {
                        new RecordField("timestamp", new StringSchema()),
                    },
                },
                new RecordSchema(nameof(OrderCreatedEvent))
                {
                    Fields = new[]
                    {
                        new RecordField("timestamp", new StringSchema()),
                        new RecordField(
                            "total",
                            new BytesSchema
                            {
                                LogicalType = new DecimalLogicalType(5, 2),
                            }),
                    },
                },
            });

            var deserialize = deserializerBuilder.BuildDelegate<OrderCreatedEvent>(schema);

            var serialize = new JsonSerializerBuilder(JsonSerializerBuilder
                .CreateDefaultCaseBuilders()
                .Prepend(builder => new OrderSerializerBuilderCase(builder)))
                .BuildDelegate<OrderEvent>(schema);

            var value = new OrderCreatedEvent
            {
                Timestamp = DateTime.UtcNow,
                Total = 40M,
            };

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            var result = deserialize(ref reader);
            Assert.Equal(value.Timestamp, result.Timestamp);
            Assert.Equal(value.Total, result.Total);
        }

        [Fact]
        public void SelectedTypes()
        {
            var schema = new RecordSchema(nameof(EventContainer))
            {
                Fields = new[]
                {
                    new RecordField(
                        "event",
                        new UnionSchema(new[]
                        {
                            new RecordSchema(nameof(OrderCreatedEvent))
                            {
                                Fields = new[]
                                {
                                    new RecordField("timestamp", new StringSchema()),
                                    new RecordField(
                                        "total",
                                        new BytesSchema()
                                        {
                                            LogicalType = new DecimalLogicalType(5, 2),
                                        }),
                                },
                            },
                            new RecordSchema(nameof(OrderCancelledEvent))
                            {
                                Fields = new[]
                                {
                                    new RecordField("timestamp", new StringSchema()),
                                },
                            },
                        })),
                },
            };

            var deserialize = new JsonDeserializerBuilder(JsonDeserializerBuilder
                .CreateDefaultCaseBuilders()
                .Prepend(builder => new OrderDeserializerBuilderCase(builder)))
                .BuildDelegate<EventContainer>(schema);

            var serialize = new JsonSerializerBuilder(JsonSerializerBuilder
                .CreateDefaultCaseBuilders()
                .Prepend(builder => new OrderSerializerBuilderCase(builder)))
                .BuildDelegate<EventContainer>(schema);

            var creation = new EventContainer
            {
                Event = new OrderCreatedEvent
                {
                    Timestamp = DateTime.UtcNow,
                    Total = 40M,
                },
            };

            var cancellation = new EventContainer
            {
                Event = new OrderCancelledEvent
                {
                    Timestamp = DateTime.UtcNow,
                },
            };

            using (stream)
            {
                serialize(creation, new Utf8JsonWriter(stream));

                var reader = new Utf8JsonReader(stream.ToArray());

                var result = deserialize(ref reader);
                Assert.IsType<OrderCreatedEvent>(result.Event);

                stream.Position = 0;
                serialize(cancellation, new Utf8JsonWriter(stream));

                reader = new Utf8JsonReader(stream.ToArray());

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
                new StringSchema(),
            });

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        public class EventContainer
        {
            public IEvent Event { get; set; }
        }

        public abstract class OrderEvent : IEvent
        {
            public DateTime Timestamp { get; set; }
        }

        public class OrderCreatedEvent : OrderEvent
        {
            public decimal Total { get; set; }
        }

        public class OrderCancelledEvent : OrderEvent
        {
        }

        public class OrderDeserializerBuilderCase : JsonUnionDeserializerBuilderCase
        {
            public OrderDeserializerBuilderCase(IJsonDeserializerBuilder builder)
                : base(builder)
            {
            }

            protected override Type SelectType(Type type, Schema schema)
            {
                if (type.IsAssignableFrom(typeof(OrderEvent)))
                {
                    return (schema as RecordSchema)?.Name switch
                    {
                        nameof(OrderCreatedEvent) => typeof(OrderCreatedEvent),
                        nameof(OrderCancelledEvent) => typeof(OrderCancelledEvent),
                        _ => throw new UnsupportedSchemaException(schema),
                    };
                }

                return base.SelectType(type, schema);
            }
        }

        public class OrderSerializerBuilderCase : JsonUnionSerializerBuilderCase
        {
            public OrderSerializerBuilderCase(IJsonSerializerBuilder builder)
                : base(builder)
            {
            }

            protected override Type SelectType(Type type, Schema schema)
            {
                if (type.IsAssignableFrom(typeof(OrderEvent)))
                {
                    return (schema as RecordSchema)?.Name switch
                    {
                        nameof(OrderCreatedEvent) => typeof(OrderCreatedEvent),
                        nameof(OrderCancelledEvent) => typeof(OrderCancelledEvent),
                        _ => throw new UnsupportedSchemaException(schema),
                    };
                }

                return base.SelectType(type, schema);
            }
        }
    }
}
