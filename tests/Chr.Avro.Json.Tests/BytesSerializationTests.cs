using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class BytesSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public BytesSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [InlineData(new byte[] { })]
        [InlineData(new byte[] { 0x00 })]
        [InlineData(new byte[] { 0xf0, 0x9f, 0x92, 0x81, 0xf0, 0x9f, 0x8e, 0x8d })]
        public void ByteArrayValues(byte[] value)
        {
            var schema = new BytesSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<byte[]>(schema);
            var serialize = _serializerBuilder.BuildDelegate<byte[]>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(GuidData))]
        public void GuidValues(Guid value)
        {
            var schema = new BytesSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<Guid>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Guid>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(GuidData))]
        public void NullableGuidValues(Guid value)
        {
            var schema = new BytesSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<Guid?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Guid>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        public static IEnumerable<object[]> GuidData => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("ed7ba470-8e54-465e-825c-99712043e01c") }
        };
    }
}
