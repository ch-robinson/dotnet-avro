using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class BooleanSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public BooleanSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void BooleanValues(bool value)
        {
            var schema = new BooleanSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<bool>(schema);
            var serializer = SerializerBuilder.BuildSerializer<bool>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }
    }
}
