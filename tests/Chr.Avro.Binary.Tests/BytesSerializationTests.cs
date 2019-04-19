using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class BytesSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public BytesSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(new byte[] { })]
        [InlineData(new byte[] { 0x00 })]
        [InlineData(new byte[] { 0xf0, 0x9f, 0x92, 0x81, 0xf0, 0x9f, 0x8e, 0x8d })]
        public void ByteArrayValues(byte[] value)
        {
            var schema = new BytesSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<byte[]>(schema);
            var serializer = SerializerBuilder.BuildSerializer<byte[]>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(GuidData))]
        public void GuidValues(Guid value)
        {
            var schema = new BytesSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<Guid>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Guid>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        public static IEnumerable<object[]> GuidData => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("ed7ba470-8e54-465e-825c-99712043e01c") }
        };
    }
}
