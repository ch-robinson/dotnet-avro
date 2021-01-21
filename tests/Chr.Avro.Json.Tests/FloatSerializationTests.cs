using Chr.Avro.Abstract;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class FloatSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public FloatSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new FloatSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(float.MinValue)]
        [InlineData(0.0)]
        [InlineData(float.MaxValue)]
        public void SingleValues(float value)
        {
            var schema = new FloatSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = _serializerBuilder.BuildDelegate<float>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
