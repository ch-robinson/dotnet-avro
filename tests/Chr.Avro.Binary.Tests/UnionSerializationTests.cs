namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class UnionSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public UnionSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public interface IEvent
        {
            DateTime Timestamp { get; }
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

        [Fact]
        public void EmptyUnionToObjectType()
        {
            var schema = new UnionSchema();

            Assert.Throws<UnsupportedSchemaException>(() => deserializerBuilder.BuildDelegate<object>(schema));
            Assert.Throws<UnsupportedSchemaException>(() => serializerBuilder.BuildDelegate<object>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntUnionEncodings))]
        public void NullAndIntUnionToInt32Type(int? value, byte[] encoding)
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
                    serialize(value.Value, new BinaryWriter(stream));
                }

                Assert.Equal(encoding, stream.ToArray());
            }

            Assert.Throws<UnsupportedTypeException>(() => deserializerBuilder.BuildDelegate<int>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntUnionEncodings))]
        public void NullAndIntUnionToNullableInt32Type(int? value, byte[] encoding)
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
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
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
                new IntSchema(),
            });

            Assert.Throws<UnsupportedTypeException>(() => serializerBuilder.BuildDelegate<string>(schema));
            Assert.Throws<UnsupportedTypeException>(() => deserializerBuilder.BuildDelegate<string>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndStringUnionEncodings))]
        public void NullAndStringUnionToStringType(string value, byte[] encoding)
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
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
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
                new NullSchema(),
            });

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
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

            var serialize = new BinarySerializerBuilder(BinarySerializerBuilder
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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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

            var deserialize = new BinaryDeserializerBuilder(BinaryDeserializerBuilder
                .CreateDefaultCaseBuilders()
                .Prepend(builder => new OrderDeserializerBuilderCase(builder)))
                .BuildDelegate<EventContainer>(schema);

            var serialize = new BinarySerializerBuilder(BinarySerializerBuilder
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
                serialize(creation, new BinaryWriter(stream));

                var reader = new BinaryReader(stream.ToArray());

                var result = deserialize(ref reader);
                Assert.IsType<OrderCreatedEvent>(result.Event);

                stream.Position = 0;
                serialize(cancellation, new BinaryWriter(stream));

                reader = new BinaryReader(stream.ToArray());

                result = deserialize(ref reader);
                Assert.IsType<OrderCancelledEvent>(result.Event);
            }
        }

        [Fact]
        public void NullableSelectedTypes()
        {
            var schema = new RecordSchema(nameof(EventContainer))
            {
                Fields = new[]
                {
                    new RecordField(
                        "event",
                        new UnionSchema(new Schema[]
                        {
                            new NullSchema(),
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

            var deserialize = new BinaryDeserializerBuilder(BinaryDeserializerBuilder
                .CreateDefaultCaseBuilders()
                .Prepend(builder => new OrderDeserializerBuilderCase(builder)))
                .BuildDelegate<EventContainer>(schema);

            var serialize = new BinarySerializerBuilder(BinarySerializerBuilder
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
                serialize(creation, new BinaryWriter(stream));

                var reader = new BinaryReader(stream.ToArray());

                var result = deserialize(ref reader);
                Assert.IsType<OrderCreatedEvent>(result.Event);

                stream.Position = 0;
                serialize(cancellation, new BinaryWriter(stream));

                reader = new BinaryReader(stream.ToArray());

                result = deserialize(ref reader);
                Assert.IsType<OrderCancelledEvent>(result.Event);
            }
        }

        [Fact]
        public void NullableSelectedTypesNull()
        {
            var schema = new RecordSchema(nameof(EventContainer))
            {
                Fields = new[]
                {
                    new RecordField(
                        "event",
                        new UnionSchema(new Schema[]
                        {
                            new NullSchema(),
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

            var deserialize = new BinaryDeserializerBuilder(BinaryDeserializerBuilder
                .CreateDefaultCaseBuilders()
                .Prepend(builder => new OrderDeserializerBuilderCase(builder)))
                .BuildDelegate<EventContainer>(schema);

            var serialize = new BinarySerializerBuilder(BinarySerializerBuilder
                .CreateDefaultCaseBuilders()
                .Prepend(builder => new OrderSerializerBuilderCase(builder)))
                .BuildDelegate<EventContainer>(schema);

            var empty = new EventContainer
            {
                Event = null,
            };

            using (stream)
            {
                serialize(empty, new BinaryWriter(stream));

                var reader = new BinaryReader(stream.ToArray());

                var result = deserialize(ref reader);
                Assert.Null(result.Event);
            }
        }

        [Theory]
        [MemberData(nameof(StringUnionEncodings))]
        public void StringUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new StringSchema(),
            });

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
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

        public class OrderDeserializerBuilderCase : BinaryUnionDeserializerBuilderCase
        {
            public OrderDeserializerBuilderCase(IBinaryDeserializerBuilder builder)
                : base(builder)
            {
            }

            protected override Type SelectType(Type type, Schema schema)
            {
                if (type.IsAssignableFrom(typeof(OrderEvent)))
                {
                    return schema switch
                    {
                        NullSchema => type,
                        RecordSchema and { Name: nameof(OrderCreatedEvent) } => typeof(OrderCreatedEvent),
                        RecordSchema and { Name: nameof(OrderCancelledEvent) } => typeof(OrderCancelledEvent),
                        _ => throw new UnsupportedSchemaException(schema),
                    };
                }

                return base.SelectType(type, schema);
            }
        }

        public class OrderSerializerBuilderCase : BinaryUnionSerializerBuilderCase
        {
            public OrderSerializerBuilderCase(IBinarySerializerBuilder builder)
                : base(builder)
            {
            }

            protected override Type SelectType(Type type, Schema schema)
            {
                if (type.IsAssignableFrom(typeof(OrderEvent)))
                {
                    return schema switch
                    {
                        NullSchema => type,
                        RecordSchema and { Name: nameof(OrderCreatedEvent) } => typeof(OrderCreatedEvent),
                        RecordSchema and { Name: nameof(OrderCancelledEvent) } => typeof(OrderCancelledEvent),
                        _ => throw new UnsupportedSchemaException(schema),
                    };
                }

                return base.SelectType(type, schema);
            }
        }
    }
}
