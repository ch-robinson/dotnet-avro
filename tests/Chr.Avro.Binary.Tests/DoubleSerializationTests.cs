using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DoubleSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public DoubleSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(double.NaN)]
        [InlineData(double.NegativeInfinity)]
        [InlineData(double.MinValue)]
        [InlineData(0.0)]
        [InlineData(double.MaxValue)]
        [InlineData(double.PositiveInfinity)]
        public void DoubleValues(double value)
        {
            var schema = new DoubleSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<double>(schema);
            var serializer = SerializerBuilder.BuildSerializer<double>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new DoubleSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<double>(schema);
            var serializer = SerializerBuilder.BuildSerializer<int>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }
    }
}
