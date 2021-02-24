namespace Chr.Avro.Serialization.Tests
{
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class FloatSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public FloatSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new FloatSchema();

            var deserialize = deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(float.MinValue)]
        [InlineData(0.0)]
        [InlineData(float.MaxValue)]
        public void SingleValues(float value)
        {
            var schema = new FloatSchema();

            var deserialize = deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = serializerBuilder.BuildDelegate<float>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
