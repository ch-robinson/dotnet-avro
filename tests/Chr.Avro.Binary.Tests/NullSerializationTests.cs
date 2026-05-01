namespace Chr.Avro.Serialization.Tests
{
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class NullSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly TestBufferWriter bufferWriter;

        public NullSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            bufferWriter = new TestBufferWriter();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(null)]
        public void DynamicObjectValues(dynamic value)
        {
            var schema = new NullSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(null, deserialize(ref reader));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        public void Int32Values(int value)
        {
            var schema = new NullSchema();

            var deserialize = deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

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

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(default, deserialize(ref reader));
        }
    }
}
