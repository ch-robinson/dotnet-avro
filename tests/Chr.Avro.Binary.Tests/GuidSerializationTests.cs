using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class GuidSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public GuidSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void GuidValues(Guid value)
        {
            var schema = new StringSchema()
            {
                LogicalType = new UuidLogicalType()
            };

            var deserializer = DeserializerBuilder.BuildDeserializer<Guid>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Guid>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void NullableGuidValues(Guid value)
        {
            var schema = new StringSchema()
            {
                LogicalType = new UuidLogicalType()
            };

            var deserializer = DeserializerBuilder.BuildDeserializer<Guid?>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Guid>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        public static IEnumerable<object[]> Guids => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("9281c70a-a916-4bad-9713-936442d7c0e8") },
        };
    }
}
