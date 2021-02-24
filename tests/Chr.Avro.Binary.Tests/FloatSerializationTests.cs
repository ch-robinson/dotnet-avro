namespace Chr.Avro.Serialization.Tests
{
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class FloatSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public FloatSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        [Theory]
        [InlineData(-5)]
        [InlineData(0)]
        [InlineData(5)]
        public void Int32Values(int value)
        {
            var schema = new FloatSchema();

            var deserialize = deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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

            var deserialize = deserializerBuilder.BuildDelegate<float>(schema);
            var serialize = serializerBuilder.BuildDelegate<float>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
