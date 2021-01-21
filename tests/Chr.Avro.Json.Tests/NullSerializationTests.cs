using Chr.Avro.Abstract;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class NullSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public NullSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        public void Int32Values(int value)
        {
            var schema = new NullSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(default, deserialize(ref reader));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("test")]
        public void StringValues(string value)
        {
            var schema = new NullSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(default, deserialize(ref reader));
        }
    }
}
