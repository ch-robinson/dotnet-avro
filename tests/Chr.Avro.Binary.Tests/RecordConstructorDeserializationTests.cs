using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Binary.Tests
{
    public class RecordConstructorDeserializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly ISchemaBuilder SchemaBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public RecordConstructorDeserializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SchemaBuilder = new SchemaBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Fact]
        public void RecordWithCyclicDependenciesAndOptionalParameters()
        {
            var schema = SchemaBuilder.BuildSchema<SourceNode>();
            var serializer = SerializerBuilder.BuildSerializer<SourceNode>(schema);
            var deserializer = DeserializerBuilder.BuildDeserializer<MappedNode>(schema);
            var sourceNode = new SourceNode()
            {
                Id = 5,
                Children = new[]
                {
                    new SourceNode()
                    {
                        Id = 9,
                        Children = new SourceNode[0]
                    },
                    new SourceNode()
                    {
                        Id = 3,
                        Children = new[]
                        {
                            new SourceNode()
                            {
                                Id = 2,
                                Children = new SourceNode[0]
                            },
                            new SourceNode()
                            {
                                Id = 10,
                                Children = new SourceNode[0]
                            }
                        }
                    }
                }
            };
            var serialized = serializer.Serialize(sourceNode);

            var n5 = deserializer.Deserialize(serialized);

            Assert.Equal(5, n5.RequiredId);
            Assert.Equal(999, n5.OptionalId);
            Assert.Collection(n5.Children,
                n9 =>
                {
                    Assert.Equal(9, n9.RequiredId);
                    Assert.Equal(999, n9.OptionalId);
                    Assert.Empty(n9.Children);
                },
                n3 =>
                {
                    Assert.Equal(3, n3.RequiredId);
                    Assert.Equal(999, n3.OptionalId);
                    Assert.Collection(n3.Children,
                        n2 =>
                        {
                            Assert.Equal(2, n2.RequiredId);
                            Assert.Equal(999, n2.OptionalId);
                            Assert.Empty(n2.Children);
                        },
                        n10 =>
                        {
                            Assert.Equal(10, n10.RequiredId);
                            Assert.Equal(999, n10.OptionalId);
                            Assert.Empty(n10.Children);
                        }
                    );
                }
            );
        }

        private class SourceNode
        {
            public IEnumerable<SourceNode> Children { get; set; }

            public int Id { get; set; }
        }

        private class MappedNode
        {
            public IEnumerable<MappedNode> Children { get; set; }

            public int RequiredId { get; set; }

            public int OptionalId { get; set; }

            public MappedNode(int id, IEnumerable<MappedNode> children, int optionalId = 999)
            {
                RequiredId = id;
                Children = children;
                OptionalId = optionalId;
            }
        }
    }
}
