using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class NullSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public NullSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        public void Int32Values(int value)
        {
            var schema = new NullSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<int>(schema);
            var serializer = SerializerBuilder.BuildSerializer<int>(schema);

            Assert.Equal(default, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("test")]
        public void StringValues(string value)
        {
            var schema = new NullSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            var serializer = SerializerBuilder.BuildSerializer<string>(schema);

            Assert.Equal(default, deserializer.Deserialize(serializer.Serialize(value)));
        }
    }
}
