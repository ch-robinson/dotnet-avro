using Chr.Avro.Abstract;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class FloatSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public FloatSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new FloatSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
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

            var deserialize = _deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = _serializerBuilder.BuildDelegate<float>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
