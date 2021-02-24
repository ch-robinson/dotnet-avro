namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class FixedSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public FixedSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> GuidData => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("ed7ba470-8e54-465e-825c-99712043e01c") },
        };

        [Theory]
        [InlineData(new byte[] { })]
        [InlineData(new byte[] { 0x00 })]
        [InlineData(new byte[] { 0xf0, 0x9f, 0x92, 0x81, 0xf0, 0x9f, 0x8e, 0x8d })]
        public void ByteArrayValues(byte[] value)
        {
            var schema = new FixedSchema("test", value.Length);

            var deserialize = deserializerBuilder.BuildDelegate<byte[]>(schema);
            var serialize = serializerBuilder.BuildDelegate<byte[]>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(GuidData))]
        public void GuidValues(Guid value)
        {
            var schema = new FixedSchema("test", 16);

            var deserialize = deserializerBuilder.BuildDelegate<Guid>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(GuidData))]
        public void NullableGuidValues(Guid value)
        {
            var schema = new FixedSchema("test", 16);

            var deserialize = deserializerBuilder.BuildDelegate<Guid?>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
