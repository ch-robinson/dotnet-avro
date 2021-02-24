namespace Chr.Avro.Serialization.Tests
{
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class NullSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public NullSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        public void Int32Values(int value)
        {
            var schema = new NullSchema();

            var deserialize = deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(default, deserialize(ref reader));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("test")]
        public void StringValues(string value)
        {
            var schema = new NullSchema();

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(default, deserialize(ref reader));
        }
    }
}
