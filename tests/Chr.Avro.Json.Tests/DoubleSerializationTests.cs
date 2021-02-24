namespace Chr.Avro.Serialization.Tests
{
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class DoubleSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DoubleSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        [Theory]
        [InlineData(double.MinValue)]
        [InlineData(0.0)]
        [InlineData(double.MaxValue)]
        public void DoubleValues(double value)
        {
            var schema = new DoubleSchema();

            var deserialize = deserializerBuilder.BuildDelegate<double>(schema);
            var serialize = serializerBuilder.BuildDelegate<double>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new DoubleSchema();

            var deserialize = deserializerBuilder.BuildDelegate<double>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
