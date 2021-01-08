using Chr.Avro.Abstract;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class BooleanSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public BooleanSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void BooleanValues(bool value)
        {
            var schema = new BooleanSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<bool>(schema);
            var serialize = _serializerBuilder.BuildDelegate<bool>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
