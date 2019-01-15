using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class FloatSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public FloatSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new FloatSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<float>(schema);
            var serializer = SerializerBuilder.BuildSerializer<int>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(float.NaN)]
        [InlineData(float.NegativeInfinity)]
        [InlineData(float.MinValue)]
        [InlineData(0.0)]
        [InlineData(float.MaxValue)]
        [InlineData(float.PositiveInfinity)]
        public void SingleValues(float value)
        {
            var schema = new FloatSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<float>(schema);
            var serializer = SerializerBuilder.BuildSerializer<float>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }
    }
}
