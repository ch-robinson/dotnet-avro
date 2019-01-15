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
        public void RecordWithMissingFields()
        {
            var schema = new RecordSchema("ThreeBooleans", new[]
            {
                new RecordField("First", new BooleanSchema()),
                new RecordField("Second", new BooleanSchema()),
                new RecordField("Third", new BooleanSchema())
            });

            var deserializer = DeserializerBuilder.BuildDeserializer<WithoutSecondField>(schema);
            var serializer = SerializerBuilder.BuildSerializer<WithSecondField>(schema);

            var value = new WithSecondField()
            {
                First = true,
                Second = false,
                Third = true
            };

            Assert.True(deserializer.Deserialize(serializer.Serialize(value)).Third);
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

        public class WithSecondField
        {
            public bool First { get; set; }

            public bool Second { get; set; }

            public bool Third { get; set; }
        }

        public class WithoutSecondField
        {
            public bool First { get; set; }

            public bool Third { get; set; }
        }
    }
}
