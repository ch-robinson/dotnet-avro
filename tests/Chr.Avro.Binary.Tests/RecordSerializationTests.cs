using Chr.Avro.Abstract;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class RecordSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public RecordSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Fact]
        public void RecordWithCyclicDependencies()
        {
            var schema = new RecordSchema("Node");
            schema.Fields.Add(new RecordField("Value", new IntSchema()));
            schema.Fields.Add(new RecordField("Children", new ArraySchema(schema)));

            var deserializer = DeserializerBuilder.BuildDeserializer<Node>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Node>(schema);

            var n5 = deserializer.Deserialize(serializer.Serialize(new Node()
            {
                Value = 5,
                Children = new[]
                {
                    new Node()
                    {
                        Value = 4,
                        Children = new Node[0]
                    },
                    new Node()
                    {
                        Value = 7,
                        Children = new[]
                        {
                            new Node()
                            {
                                Value = 6,
                                Children = new Node[0]
                            },
                            new Node
                            {
                                Value = 8,
                                Children = new Node[0]
                            }
                        }
                    }
                }
            }));

            Assert.Equal(5, n5.Value);
            Assert.Collection(n5.Children,
                n4 =>
                {
                    Assert.Equal(4, n4.Value);
                    Assert.Empty(n4.Children);
                },
                n7 =>
                {
                    Assert.Equal(7, n7.Value);
                    Assert.Collection(n7.Children,
                        n6 =>
                        {
                            Assert.Equal(6, n6.Value);
                            Assert.Empty(n6.Children);
                        },
                        n8 =>
                        {
                            Assert.Equal(8, n8.Value);
                            Assert.Empty(n8.Children);
                        }
                    );
                }
            );
        }

        [Fact]
        public void RecordWithCyclicDependenciesAndOptionalParameters()
        {
            var schema = new RecordSchema("Node");
            schema.Fields.Add(new RecordField("Value", new IntSchema()));
            schema.Fields.Add(new RecordField("Children", new ArraySchema(schema)));

            var serializer = SerializerBuilder.BuildSerializer<Node>(schema);
            var deserializer = DeserializerBuilder.BuildDeserializer<MappedNode>(schema);

            var n5 = deserializer.Deserialize(serializer.Serialize(new Node()
            {
                Value = 5,
                Children = new[]
                {
                    new Node()
                    {
                        Value = 9,
                        Children = new Node[0]
                    },
                    new Node()
                    {
                        Value = 3,
                        Children = new[]
                        {
                            new Node()
                            {
                                Value = 2,
                                Children = new Node[0]
                            },
                            new Node()
                            {
                                Value = 10,
                                Children = new Node[0]
                            }
                        }
                    }
                }
            }));

            Assert.Equal(5, n5.RequiredValue);
            Assert.Equal(999, n5.OptionalValue);
            Assert.Collection(n5.Children,
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
                    Assert.Collection(n3.Children,
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
                        }
                    );
                }
            );
        }

        [Fact]
        public void RecordWithMissingFields()
        {
            var boolean = new BooleanSchema();
            var array = new ArraySchema(boolean);
            var map = new MapSchema(new IntSchema());
            var @enum = new EnumSchema("Position", new[] { "First", "Last" });
            var union = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                array
            });

            var schema = new RecordSchema("AllFields", new[]
            {
                new RecordField("First", union),
                new RecordField("Second", union),
                new RecordField("Third", array),
                new RecordField("Fourth", array),
                new RecordField("Fifth", map),
                new RecordField("Sixth", map),
                new RecordField("Seventh", @enum),
                new RecordField("Eighth", @enum)
            });

            var deserializer = DeserializerBuilder.BuildDeserializer<WithoutEvenFields>(schema);
            var serializer = SerializerBuilder.BuildSerializer<WithEvenFields>(schema);

            var value = new WithEvenFields()
            {
                First = new List<bool>() { false },
                Second = new List<bool>() { false, false },
                Third = new List<bool>() { false, false, false },
                Fourth = new List<bool>() { false },
                Fifth = new Dictionary<string, int>() { { "first", 1 } },
                Sixth = new Dictionary<string, int>() { { "first", 1 }, { "second", 2 } },
                Seventh = Position.Last,
                Eighth = Position.First
            };

            Assert.Equal(value.Seventh, deserializer.Deserialize(serializer.Serialize(value)).Seventh);
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

            var deserializer = DeserializerBuilder.BuildDeserializer<Reference>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Reference>(schema);

            var root = deserializer.Deserialize(serializer.Serialize(new Reference()
            {
                Node = new Node()
                {
                    Children = new Node[0]
                },
                RelatedNodes = new Node[0]
            }));

            Assert.Empty(root.Node.Children);
            Assert.Empty(root.RelatedNodes);
        }

        public class MappedNode
        {
            public int RequiredValue { get; set; }

            public int OptionalValue { get; set; }

            public IEnumerable<MappedNode> Children { get; set; }

            public MappedNode(int value, IEnumerable<MappedNode> children, int optionalValue = 999)
            {
                Children = children;
                OptionalValue = optionalValue;
                RequiredValue = value;
            }
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

            public Position Seventh { get; set; }

            public Position Eighth { get; set; }
        }

        public class WithoutEvenFields
        {
            public IEnumerable<bool> First { get; set; }

            public IEnumerable<bool> Third { get; set; }

            public IDictionary<string, int> Fifth { get; set; }

            public Position Seventh { get; set; }
        }

        public enum Position
        {
            First,
            Last
        }
    }
}
