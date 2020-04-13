using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class UnionSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public UnionSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Fact]
        public void EmptyUnionToObjectType()
        {
            var schema = new UnionSchema();

            Assert.Throws<UnsupportedSchemaException>(() => SerializerBuilder.BuildSerializer<object>(schema));
            Assert.Throws<UnsupportedSchemaException>(() => DeserializerBuilder.BuildDeserializer<object>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntUnionEncodings))]
        public void NullAndIntUnionToInt32Type(int? value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<int>(schema);

            if (value.HasValue)
            {
                Assert.Equal(encoding, serializer.Serialize(value.Value));
            }

            Assert.Throws<UnsupportedTypeException>(() => DeserializerBuilder.BuildDeserializer<int>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntUnionEncodings))]
        public void NullAndIntUnionToNullableInt32Type(int? value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<int?>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<int?>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Fact]
        public void NullAndIntUnionToStringType()
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            Assert.Throws<UnsupportedTypeException>(() => SerializerBuilder.BuildSerializer<string>(schema));
            Assert.Throws<UnsupportedTypeException>(() => DeserializerBuilder.BuildDeserializer<string>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndStringUnionEncodings))]
        public void NullAndStringUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new StringSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<string>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Theory]
        [MemberData(nameof(NullUnionEncodings))]
        public void NullUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<string>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
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

            var codec = new BinaryCodec();
            var resolver = new ReflectionResolver();

            var deserializer = DeserializerBuilder.BuildDeserializer<OrderCreatedEvent>(schema);

            var serializer = new BinarySerializerBuilder(BinarySerializerBuilder.CreateBinarySerializerCaseBuilders(codec)
                .Prepend(builder => new OrderSerializerBuilderCase(resolver, codec, builder)))
                .BuildSerializer<OrderEvent>(schema);

            var value = new OrderCreatedEvent
            {
                Timestamp = DateTime.UtcNow,
                Total = 40M
            };

            var result = deserializer.Deserialize(serializer.Serialize(value));
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

            var codec = new BinaryCodec();
            var resolver = new ReflectionResolver();

            var deserializer = new BinaryDeserializerBuilder(BinaryDeserializerBuilder.CreateBinaryDeserializerCaseBuilders(codec)
                .Prepend(builder => new OrderDeserializerBuilderCase(resolver, codec, builder)))
                .BuildDeserializer<EventContainer>(schema);

            var serializer = new BinarySerializerBuilder(BinarySerializerBuilder.CreateBinarySerializerCaseBuilders(codec)
                .Prepend(builder => new OrderSerializerBuilderCase(resolver, codec, builder)))
                .BuildSerializer<EventContainer>(schema);

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

            Assert.IsType<OrderCreatedEvent>(deserializer.Deserialize(serializer.Serialize(creation)).Event);
            Assert.IsType<OrderCancelledEvent>(deserializer.Deserialize(serializer.Serialize(cancellation)).Event);
        }

        [Theory]
        [MemberData(nameof(StringUnionEncodings))]
        public void StringUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new StringSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<string>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        public static IEnumerable<object[]> NullAndIntUnionEncodings => new List<object[]>
        {
            new object[] { null, new byte[] { 0x00 } },
            new object[] { 2, new byte[] { 0x02, 0x04 } },
        };

        public static IEnumerable<object[]> NullAndStringUnionEncodings => new List<object[]>
        {
            new object[] { null, new byte[] { 0x00 } },
            new object[] { "test", new byte[] { 0x02, 0x08, 0x74, 0x65, 0x73, 0x74 } },
        };

        public static IEnumerable<object[]> NullUnionEncodings => new List<object[]>
        {
            new object[] { null, new byte[] { 0x00 } },
        };

        public static IEnumerable<object[]> StringUnionEncodings => new List<object[]>
        {
            new object[] { "test", new byte[] { 0x00, 0x08, 0x74, 0x65, 0x73, 0x74 } },
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

            public OrderSerializerBuilderCase(ITypeResolver resolver, IBinaryCodec codec, IBinarySerializerBuilder builder) : base(codec, builder)
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
