using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class UnionSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public UnionSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
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
        [MemberData(nameof(NullAndIntUnionEncodings))]
        public void NullAndIntUnionToInt32Type(int? value, byte[] encoding)
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
                    serialize(value.Value, new BinaryWriter(_stream));
                }

                Assert.Equal(encoding, _stream.ToArray());
            }

            Assert.Throws<UnsupportedTypeException>(() => _deserializerBuilder.BuildDelegate<int>(schema));
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

            var deserialize = _deserializerBuilder.BuildDelegate<int?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int?>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
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
        [MemberData(nameof(NullAndStringUnionEncodings))]
        public void NullAndStringUnionToStringType(string value, byte[] encoding)
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
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(NullUnionEncodings))]
        public void NullUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema()
            });

            var deserialize = _deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
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

            var serialize = new BinarySerializerBuilder(BinarySerializerBuilder.DefaultCaseBuilders
                .Prepend(builder => new OrderSerializerBuilderCase(resolver, builder)))
                .BuildDelegate<OrderEvent>(schema);

            var value = new OrderCreatedEvent
            {
                Timestamp = DateTime.UtcNow,
                Total = 40M
            };

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

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

            var deserialize = new BinaryDeserializerBuilder(BinaryDeserializerBuilder.DefaultCaseBuilders
                .Prepend(builder => new OrderDeserializerBuilderCase(resolver, builder)))
                .BuildDelegate<EventContainer>(schema);

            var serialize = new BinarySerializerBuilder(BinarySerializerBuilder.DefaultCaseBuilders
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
                serialize(creation, new BinaryWriter(_stream));

                var reader = new BinaryReader(_stream.ToArray());

                var result = deserialize(ref reader);
                Assert.IsType<OrderCreatedEvent>(result.Event);

                _stream.Position = 0;
                serialize(cancellation, new BinaryWriter(_stream));

                reader = new BinaryReader(_stream.ToArray());

                result = deserialize(ref reader);
                Assert.IsType<OrderCancelledEvent>(result.Event);
            }
        }

        [Theory]
        [MemberData(nameof(StringUnionEncodings))]
        public void StringUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new StringSchema()
            });

            var deserialize = _deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
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

            public OrderDeserializerBuilderCase(ITypeResolver resolver, IBinaryDeserializerBuilder builder) : base(builder)
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

            public OrderSerializerBuilderCase(ITypeResolver resolver, IBinarySerializerBuilder builder) : base(builder)
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
