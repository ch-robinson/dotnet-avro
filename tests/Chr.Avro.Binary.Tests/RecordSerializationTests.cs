namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class RecordSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public RecordSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        [Fact]
        public void RecordWithAnonymousType()
        {
            var boolean = new BooleanSchema();
            var array = new ArraySchema(boolean);
            var map = new MapSchema(new IntSchema());
            var @enum = new EnumSchema("Ordinal", new[] { "None", "First", "Second", "Third", "Fourth" });
            var union = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                array,
            });

            var schema = new RecordSchema("AllFields")
            {
                Fields = new[]
                {
                    new RecordField("First", union),
                    new RecordField("Second", union),
                    new RecordField("Third", array),
                    new RecordField("Fourth", array),
                    new RecordField("Fifth", map),
                    new RecordField("Sixth", map),
                    new RecordField("Seventh", @enum),
                    new RecordField("Eighth", @enum),
                },
            };

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            var value = new
            {
                First = new List<bool>() { false },
                Second = new List<bool>() { false, false },
                Third = new List<bool>() { false, false, false },
                Fourth = new List<bool>() { false },
                Fifth = new Dictionary<string, int>() { { "first", 1 } },
                Sixth = new Dictionary<string, int>() { { "first", 1 }, { "second", 2 } },
                Seventh = ImplicitEnum.First,
                Eighth = ImplicitEnum.None,
            };

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value.Seventh.ToString(), deserialize(ref reader).Seventh);
        }

        [Fact]
        public void RecordWithCyclicDependencies()
        {
            var schema = new RecordSchema("Node");
            schema.Fields.Add(new RecordField("Value", new IntSchema()));
            schema.Fields.Add(new RecordField("Children", new ArraySchema(schema)));

            var deserialize = deserializerBuilder.BuildDelegate<Node>(schema);
            var serialize = serializerBuilder.BuildDelegate<Node>(schema);

            using (stream)
            {
                serialize(
                    new Node()
                    {
                        Value = 5,
                        Children = new[]
                        {
                            new Node()
                            {
                                Value = 4,
                                Children = Array.Empty<Node>(),
                            },
                            new Node()
                            {
                                Value = 7,
                                Children = new[]
                                {
                                    new Node()
                                    {
                                        Value = 6,
                                        Children = Array.Empty<Node>(),
                                    },
                                    new Node
                                    {
                                        Value = 8,
                                        Children = Array.Empty<Node>(),
                                    },
                                },
                            },
                        },
                    },
                    new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            var n5 = deserialize(ref reader);

            Assert.Equal(5, n5.Value);
            Assert.Collection(
                n5.Children,
                n4 =>
                {
                    Assert.Equal(4, n4.Value);
                    Assert.Empty(n4.Children);
                },
                n7 =>
                {
                    Assert.Equal(7, n7.Value);
                    Assert.Collection(
                        n7.Children,
                        n6 =>
                        {
                            Assert.Equal(6, n6.Value);
                            Assert.Empty(n6.Children);
                        },
                        n8 =>
                        {
                            Assert.Equal(8, n8.Value);
                            Assert.Empty(n8.Children);
                        });
                });
        }

        [Fact]
        public void RecordWithCyclicDependenciesAndOptionalParameters()
        {
            var schema = new RecordSchema("Node");
            schema.Fields.Add(new RecordField("Value", new IntSchema()));
            schema.Fields.Add(new RecordField("Children", new ArraySchema(schema)));

            var deserialize = deserializerBuilder.BuildDelegate<MappedNode>(schema);
            var serialize = serializerBuilder.BuildDelegate<Node>(schema);

            using (stream)
            {
                serialize(
                    new Node()
                    {
                        Value = 5,
                        Children = new[]
                        {
                            new Node()
                            {
                                Value = 9,
                                Children = Array.Empty<Node>(),
                            },
                            new Node()
                            {
                                Value = 3,
                                Children = new[]
                                {
                                    new Node()
                                    {
                                        Value = 2,
                                        Children = Array.Empty<Node>(),
                                    },
                                    new Node()
                                    {
                                        Value = 10,
                                        Children = Array.Empty<Node>(),
                                    },
                                },
                            },
                        },
                    },
                    new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            var n5 = deserialize(ref reader);

            Assert.Equal(5, n5.RequiredValue);
            Assert.Equal(999, n5.OptionalValue);
            Assert.Collection(
                n5.Children,
                n9 =>
                {
                    Assert.Equal(9, n9.RequiredValue);
                    Assert.Equal(999, n9.OptionalValue);
                    Assert.Empty(n9.Children);
                },
                n3 =>
                {
                    Assert.Equal(3, n3.RequiredValue);
                    Assert.Equal(999, n3.OptionalValue);
                    Assert.Collection(
                        n3.Children,
                        n2 =>
                        {
                            Assert.Equal(2, n2.RequiredValue);
                            Assert.Equal(999, n2.OptionalValue);
                            Assert.Empty(n2.Children);
                        },
                        n10 =>
                        {
                            Assert.Equal(10, n10.RequiredValue);
                            Assert.Equal(999, n10.OptionalValue);
                            Assert.Empty(n10.Children);
                        });
                });
        }

        [Fact]
        public void RecordWithDefaultFields()
        {
            var boolean = new BooleanSchema();
            var array = new ArraySchema(boolean);
            var map = new MapSchema(new IntSchema());
            var @enum = new EnumSchema("Ordinal", new[] { "None", "First", "Second", "Third", "Fourth" });
            var union = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                array,
            });

            var schema = new RecordSchema("AllFields")
            {
                Fields = new[]
                {
                    new RecordField("Second", union)
                    {
                        Default = new ObjectDefaultValue<List<bool>>(null, union),
                    },
                    new RecordField("Fourth", array)
                    {
                        Default = new ObjectDefaultValue<List<bool>>(new List<bool>() { false }, array),
                    },
                    new RecordField("Sixth", map)
                    {
                        Default = new ObjectDefaultValue<Dictionary<string, int>>(new Dictionary<string, int>() { { "first", 1 }, { "second", 2 } }, map),
                    },
                    new RecordField("Eighth", @enum)
                    {
                        Default = new ObjectDefaultValue<ImplicitEnum>(ImplicitEnum.None, @enum),
                    },
                },
            };

            var deserialize = deserializerBuilder.BuildDelegate<WithEvenFields>(schema);
            var serialize = serializerBuilder.BuildDelegate<WithoutEvenFields>(schema);

            var value = new WithoutEvenFields()
            {
                First = new List<bool>() { false },
                Third = new List<bool>() { false, false, false },
                Fifth = new Dictionary<string, int>() { { "first", 1 } },
                Seventh = ImplicitEnum.First,
            };

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            var with = deserialize(ref reader);

            Assert.Null(with.Second);
            Assert.Equal(new List<bool>() { false }, with.Fourth);
            Assert.Equal(new Dictionary<string, int>() { { "first", 1 }, { "second", 2 } }, with.Sixth);
            Assert.Equal(ImplicitEnum.None, with.Eighth);
        }

        [Fact]
        public void RecordWithDynamicType()
        {
            var boolean = new BooleanSchema();
            var array = new ArraySchema(boolean);
            var map = new MapSchema(new IntSchema());
            var @enum = new EnumSchema("Ordinal", new[] { "None", "First", "Second", "Third", "Fourth" });
            var union = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                array,
            });

            var schema = new RecordSchema("AllFields")
            {
                Fields = new[]
                {
                    new RecordField("First", union),
                    new RecordField("Second", union),
                    new RecordField("Third", array),
                    new RecordField("Fourth", array),
                    new RecordField("Fifth", map),
                    new RecordField("Sixth", map),
                    new RecordField("Seventh", @enum),
                    new RecordField("Eighth", @enum),
                },
            };

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            var value = new WithEvenFields()
            {
                First = new List<bool>() { false },
                Second = new List<bool>() { false, false },
                Third = new List<bool>() { false, false, false },
                Fourth = new List<bool>() { false },
                Fifth = new Dictionary<string, int>() { { "first", 1 } },
                Sixth = new Dictionary<string, int>() { { "first", 1 }, { "second", 2 } },
                Seventh = ImplicitEnum.First,
                Eighth = ImplicitEnum.None,
            };

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value.Seventh.ToString(), deserialize(ref reader).Seventh);
        }

        [Fact]
        public void RecordWithMissingFields()
        {
            var boolean = new BooleanSchema();
            var array = new ArraySchema(boolean);
            var map = new MapSchema(new IntSchema());
            var @enum = new EnumSchema("Ordinal", new[] { "None", "First", "Second", "Third", "Fourth" });
            var union = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                array,
            });

            var schema = new RecordSchema("AllFields")
            {
                Fields = new[]
                {
                    new RecordField("First", union),
                    new RecordField("Second", union),
                    new RecordField("Third", array),
                    new RecordField("Fourth", array),
                    new RecordField("Fifth", map),
                    new RecordField("Sixth", map),
                    new RecordField("Seventh", @enum),
                    new RecordField("Eighth", @enum),
                },
            };

            var deserialize = deserializerBuilder.BuildDelegate<WithoutEvenFields>(schema);
            var serialize = serializerBuilder.BuildDelegate<WithEvenFields>(schema);

            var value = new WithEvenFields()
            {
                First = new List<bool>() { false },
                Second = new List<bool>() { false, false },
                Third = new List<bool>() { false, false, false },
                Fourth = new List<bool>() { false },
                Fifth = new Dictionary<string, int>() { { "first", 1 } },
                Sixth = new Dictionary<string, int>() { { "first", 1 }, { "second", 2 } },
                Seventh = ImplicitEnum.First,
                Eighth = ImplicitEnum.None,
            };

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value.Seventh, deserialize(ref reader).Seventh);
        }

        [Fact]
        public void RecordWithMissingRecordArrayFields()
        {
            var subRecord = new RecordSchema("SubRecord")
            {
                Fields = new[]
                {
                    new RecordField("Id", new IntSchema()),
                },
            };
            var subRecordArray = new ArraySchema(subRecord);

            var defaultSubRecordArray = new ObjectDefaultValue<object[]>(Array.Empty<object>(), subRecordArray);
            var schema = new RecordSchema("AllFields")
            {
                Fields = new[]
                {
                    new RecordField("Name", new StringSchema()),
                    new RecordField("SubRecordArray", subRecordArray) { Default = defaultSubRecordArray },
                },
            };

            var serialize = serializerBuilder.BuildDelegate<SimpleRecord>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<RecordWithSubRecordArray>(schema);

            var value = new SimpleRecord()
            {
                Name = "Bob",
            };

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var deserialized = deserialize(ref reader);
            Assert.Equal(value.Name, deserialized.Name);
            Assert.Empty(deserialized.SubRecordArray);
        }

        [Fact]
        public void RecordWithDynamicRecordArray()
        {
            var boolean = new BooleanSchema();
            var array = new ArraySchema(boolean);
            var map = new MapSchema(new IntSchema());
            var @enum = new EnumSchema("Ordinal", new[] { "None", "First", "Second", "Third", "Fourth" });
            var union = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                array,
            });

            var propertiesArray = new ArraySchema(new RecordSchema("PropertiesRecord", new[]
            {
                new RecordField("Id", new IntSchema()),
                new RecordField("Address", new StringSchema()),
            }));

            var schema = new RecordSchema("AllFields")
            {
                Fields = new[]
                {
                    new RecordField("Name", new StringSchema()),
                    new RecordField("Age", new IntSchema()),
                    new RecordField("Properties", propertiesArray),
                },
            };

            var deserialize = deserializerBuilder.BuildDelegate<RecordWithDynamicArray>(schema);
            var serialize = serializerBuilder.BuildDelegate<RecordWithDynamicArray>(schema);

            var value = new RecordWithDynamicArray()
            {
                Name = "Bob",
                Age = 44,
                Properties = new[]
                {
                    new { Id = 21312, Address = "London", Weigth = 33, },
                },
            };

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var deserialized = deserialize(ref reader);
            Assert.Equal(value.Name, deserialized.Name);
            Assert.Equal(value.Age, deserialized.Age);
            Assert.Single(deserialized.Properties);

            var properties = deserialized.Properties[0];
            Assert.Equal(21312, properties.Id);
            Assert.Equal("London", properties.Address);
        }

        [Fact]
        public void RecordWithParallelDependencies()
        {
            var node = new RecordSchema("Node");
            node.Fields.Add(new RecordField("Value", new IntSchema()));
            node.Fields.Add(new RecordField("Children", new ArraySchema(node)));

            var schema = new RecordSchema("Reference");
            schema.Fields.Add(new RecordField("Node", node));
            schema.Fields.Add(new RecordField("RelatedNodes", new ArraySchema(node)));

            var deserialize = deserializerBuilder.BuildDelegate<Reference>(schema);
            var serialize = serializerBuilder.BuildDelegate<Reference>(schema);

            using (stream)
            {
                serialize(
                    new Reference()
                    {
                        Node = new Node()
                        {
                            Children = Array.Empty<Node>(),
                        },
                        RelatedNodes = Array.Empty<Node>(),
                    },
                    new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            var root = deserialize(ref reader);

            Assert.Empty(root.Node.Children);
            Assert.Empty(root.RelatedNodes);
        }

        public class MappedNode
        {
            public MappedNode(int value, IEnumerable<MappedNode> children, int optionalValue = 999)
            {
                Children = children;
                OptionalValue = optionalValue;
                RequiredValue = value;
            }

            public int RequiredValue { get; set; }

            public int OptionalValue { get; set; }

            public IEnumerable<MappedNode> Children { get; set; }
        }

        public class Node
        {
            public int Value { get; set; }

            public IEnumerable<Node> Children { get; set; }
        }

        public class Reference
        {
            public Node Node { get; set; }

            public IEnumerable<Node> RelatedNodes { get; set; }
        }

        public class WithEvenFields
        {
            public IEnumerable<bool> First { get; set; }

            public IEnumerable<bool> Second { get; set; }

            public IEnumerable<bool> Third { get; set; }

            public IEnumerable<bool> Fourth { get; set; }

            public IDictionary<string, int> Fifth { get; set; }

            public IDictionary<string, int> Sixth { get; set; }

            public ImplicitEnum Seventh { get; set; }

            public ImplicitEnum Eighth { get; set; }
        }

        public class WithoutEvenFields
        {
            public IEnumerable<bool> First { get; set; }

            public IEnumerable<bool> Third { get; set; }

            public IDictionary<string, int> Fifth { get; set; }

            public ImplicitEnum Seventh { get; set; }
        }

        public class SimpleRecord
        {
            public string Name { get; set; }
        }

        public class RecordWithSubRecordArray
        {
            public string Name { get; set; }

            public WithoutEvenFields[] SubRecordArray { get; set; }
        }

        public class RecordWithDynamicArray
        {
            public string Name { get; set; }

            public int Age { get; set; }

            public dynamic[] Properties { get; set; }
        }
    }
}
