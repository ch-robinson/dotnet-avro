namespace Chr.Avro.Serialization.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class DoubleSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DoubleSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> Doubles => new List<object[]>
        {
            new object[] { double.NaN },
            new object[] { double.NegativeInfinity },
            new object[] { double.MinValue },
            new object[] { 0.0 },
            new object[] { double.MaxValue },
            new object[] { double.PositiveInfinity },
        };

        public static IEnumerable<object[]> Integers => new List<object[]>
        {
            new object[] { -5 },
            new object[] { 0 },
            new object[] { 5 },
        };

        [Theory]
        [MemberData(nameof(Doubles))]
        public void DoubleValues(double value)
        {
            var schema = new DoubleSchema();

            var deserialize = deserializerBuilder.BuildDelegate<double>(schema);
            var serialize = serializerBuilder.BuildDelegate<double>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Doubles))]
        [MemberData(nameof(Integers))]
        public void DynamicDoubleValues(dynamic value)
        {
            var schema = new DoubleSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal((double)value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Integers))]
        public void Int32Values(int value)
        {
            var schema = new DoubleSchema();

            var deserialize = deserializerBuilder.BuildDelegate<double>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
