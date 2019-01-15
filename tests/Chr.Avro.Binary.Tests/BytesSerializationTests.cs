using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class BytesSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public BytesSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(new byte[] { })]
        [InlineData(new byte[] { 0x00 })]
        [InlineData(new byte[] { 0xf0, 0x9f, 0x92, 0x81, 0xf0, 0x9f, 0x8e, 0x8d })]
        public void ByteArrayValues(byte[] value)
        {
            var schema = new BytesSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<byte[]>(schema);
            var serializer = SerializerBuilder.BuildSerializer<byte[]>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }
    }
}
