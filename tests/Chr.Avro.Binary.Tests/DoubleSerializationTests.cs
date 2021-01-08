using Chr.Avro.Abstract;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DoubleSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public DoubleSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
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

            var deserialize = _deserializerBuilder.BuildDelegate<double>(schema);
            var serialize = _serializerBuilder.BuildDelegate<double>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new DoubleSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<double>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
