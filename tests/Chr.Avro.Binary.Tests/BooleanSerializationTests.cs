namespace Chr.Avro.Serialization.Tests
{
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class BooleanSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly TestBufferWriter bufferWriter;

        public BooleanSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            bufferWriter = new TestBufferWriter();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void BooleanValues(bool value)
        {
            var schema = new BooleanSchema();

            var deserialize = deserializerBuilder.BuildDelegate<bool>(schema);
            var serialize = serializerBuilder.BuildDelegate<bool>(schema);

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void DynamicBooleanValues(bool value)
        {
            var schema = new BooleanSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
