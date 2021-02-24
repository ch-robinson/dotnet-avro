namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class GuidSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public GuidSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> Guids => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("9281c70a-a916-4bad-9713-936442d7c0e8") },
        };

        [Theory]
        [MemberData(nameof(Guids))]
        public void GuidValues(Guid value)
        {
            var schema = new StringSchema()
            {
                LogicalType = new UuidLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<Guid>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void NullableGuidValues(Guid value)
        {
            var schema = new StringSchema()
            {
                LogicalType = new UuidLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<Guid?>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
