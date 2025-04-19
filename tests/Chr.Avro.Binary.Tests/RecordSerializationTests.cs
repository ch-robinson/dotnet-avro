namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class RecordSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private MemoryStream stream;

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
                    new RecordField("Ninth", boolean)
                    {
                        Default = new ObjectDefaultValue<bool>(true, boolean),
                    },
                },
            };

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(
                    new
                    {
                        First = new List<bool>() { false },
                        Second = new List<bool>() { false, false },
                        Third = new List<bool>() { false, false, false },
                        Fourth = new List<bool>() { false },
                        Fifth = new Dictionary<string, int>() { { "first", 1 } },
                        Sixth = new Dictionary<string, int>() { { "first", 1 }, { "second", 2 } },
                        Seventh = ImplicitEnum.First,
                        Eighth = ImplicitEnum.None,
                    },
                    new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var value = deserialize(ref reader);

            Assert.Equal(nameof(ImplicitEnum.First), value.Seventh);
            Assert.Equal(true, value.Ninth);
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
            Assert.Null(n5.NullableValue);
            Assert.Collection(
                n5.Children,
                n9 =>
                {
                    Assert.Equal(9, n9.RequiredValue);
                    Assert.Equal(999, n9.OptionalValue);
                    Assert.Null(n9.NullableValue);
                    Assert.Empty(n9.Children);
                },
                n3 =>
                {
                    Assert.Equal(3, n3.RequiredValue);
                    Assert.Equal(999, n3.OptionalValue);
                    Assert.Null(n3.NullableValue);
                    Assert.Collection(
                        n3.Children,
                        n2 =>
                        {
                            Assert.Equal(2, n2.RequiredValue);
                            Assert.Equal(999, n2.OptionalValue);
                            Assert.Null(n2.NullableValue);
                            Assert.Empty(n2.Children);
                        },
                        n10 =>
                        {
                            Assert.Equal(10, n10.RequiredValue);
                            Assert.Equal(999, n10.OptionalValue);
                            Assert.Null(n10.NullableValue);
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

        [Fact]
        public void RecordWithNewFieldDeserializedIntoTypeWithDefaultConstructor()
        {
            // schema: int A, string B, double C = 0
            // records: {A a, B b = null}, {A a}
            // other: class Mapped {
            //     Mapped(int a) { string B {get;set;} }
            var schema = new RecordSchema("Person")
            {
                Fields = new[]
                {
                    new RecordField("Name", new StringSchema()),
                    new RecordField("Age", new IntSchema()),
                    new RecordField("Address", new StringSchema())
                    {
                        Default = new ObjectDefaultValue<string>(string.Empty, new StringSchema()),
                    },
                },
            };

            // The entity class has a default constructor and read/write properties
            // but Address is missing
            var person = new PersonWithDefaultConstructor() { Name = "Bob", Age = 30 };
            var deserialized = SerializeAndDeserialize(person, schema);

            Assert.Equivalent(person, deserialized);
        }

        [Fact]
        public void RecordWithNewFieldDeserializedIntoTypeWithPartialMatchConstructor()
        {
            // schema: int A, string B, double C = 0
            // records: {A a, B b = null}, {A a}
            // other: class Mapped {
            //     Mapped(int a) { string B {get;set;} }
            var schema = new RecordSchema("Person")
            {
                Fields = new[]
                {
                    new RecordField("Name", new StringSchema()),
                    new RecordField("Age", new IntSchema()),
                    new RecordField("Address", new StringSchema())
                    {
                        Default = new ObjectDefaultValue<string>(string.Empty, new StringSchema()),
                    },
                },
            };

            // The entity doesn't have a default constructor, but takes the Name as
            // a constructor parameter
            var person = new PersonWithoutDefaultConstructor("Bob") { Age = 30 };
            var deserialized = SerializeAndDeserialize(person, schema);

            Assert.Equivalent(person, deserialized);
        }

        [Fact]
        public void RecordWithNewFieldDeserializedWithDefaultNullableValue()
        {
            // schema: int A, string B, double C = 0
            // records: {A a, B b = null}, {A a}
            // other: class Mapped {
            //     Mapped(int a) { string B {get;set;} }
            var schema = new RecordSchema("Person")
            {
                Fields = new[]
                {
                    new RecordField("Name", new StringSchema()),
                },
            };

            // The entity doesn't have a default constructor, but takes the Name as
            // a constructor parameter
            var person = new PersonWithDefaultNullableValue("Bob");
            var deserialized = SerializeAndDeserialize(person, schema);

            Assert.Equivalent(person, deserialized);
        }

        // TODO: Add test where the class has multiple constructors; pick the one that matches more fields from the record
        [Fact]
        public void RecordWithCustomDeserialization()
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
                    new RecordField("Id", new IntSchema()),
                    new RecordField("Name", new StringSchema()),
                    new RecordField("Age", new IntSchema()),
                },
            };

            // Use a deserializer that picks a particular constructor in the deserialized class
            var builders = BinaryDeserializerBuilder.CreateDefaultCaseBuilders().ToList();
            builders.Insert(0, b => new CustomConstructorPickerRecordDeserializerCase(b));
            var customDeserializerBuilder = new BinaryDeserializerBuilder(builders);

            var deserialize = customDeserializerBuilder.BuildDelegate<MultipleConstructorsRecord>(schema);
            var serialize = serializerBuilder.BuildDelegate<MultipleConstructorsRecord>(schema);

            var value = new MultipleConstructorsRecord
            {
                Id = 123,
                Name = "Alice",
                Age = 24,
            };

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            var expected = new MultipleConstructorsRecord
            {
                Id = 42,
                Name = "ALICE",
                Age = 22,
            };

            Assert.Equivalent(expected, deserialize(ref reader));
        }

        [Fact]
        public void RecordWithDefaultConstructor()
        {
            var schema = new RecordSchema("Person")
            {
                Fields = new[]
                {
                    new RecordField("Name", new StringSchema()),
                    new RecordField("Age", new IntSchema()),
                },
            };
            var person = new PersonWithDefaultConstructor() { Name = "Bob", Age = 30 };
            var deserialized = SerializeAndDeserialize(person, schema);
            Assert.Equivalent(person, deserialized);
        }

        private T SerializeAndDeserialize<T>(T item, RecordSchema schema)
        {
            var deserialize = deserializerBuilder.BuildDelegate<T>(schema);
            var serialize = serializerBuilder.BuildDelegate<T>(schema);

            using var memoryStream = new MemoryStream();

            serialize(item, new BinaryWriter(memoryStream));
            var reader = new BinaryReader(memoryStream.ToArray());

            var root = deserialize(ref reader);
            return root;
        }

        public class MappedNode
        {
            public MappedNode(int value, IEnumerable<MappedNode> children, int optionalValue = 999, double? nullableValue = null)
            {
                Children = children;
                OptionalValue = optionalValue;
                NullableValue = nullableValue;
                RequiredValue = value;
            }

            public int RequiredValue { get; set; }

            public int OptionalValue { get; set; }

            public double? NullableValue { get; set; }

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

        public class PersonWithoutDefaultConstructor
        {
            public PersonWithoutDefaultConstructor(string name)
            {
                Name = name;
            }

            public string Name { get; }

            public int Age { get; set; }
        }

        public class PersonWithDefaultConstructor
        {
            public string Name { get; set; }

            public int Age { get; set; }
        }

        public class MultipleConstructorsRecord
        {
            public MultipleConstructorsRecord()
            {
            }

            public MultipleConstructorsRecord(int id, string name, int age, string city = "Paris")
            {
                Id = id;
                Name = name?.ToUpperInvariant();
                Age = age;
                City = city;
            }

            [ChrPreferredConstructor]
            public MultipleConstructorsRecord(int id, string name, int age)
            {
                Id = 42;
                Name = name?.ToUpperInvariant();
                Age = age - 2;
            }

            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }

            public string City { get; set; }
        }

        public record PersonWithDefaultNullableValue
        {
            public PersonWithDefaultNullableValue(string name, double? age = null)
            {
                Name = name;
                Age = age;
            }

            public string Name { get; private set; }

            public double? Age { get; private set; }
        }
    }
}
