namespace Chr.Avro.Serialization.Tests
{
    using System.Collections.Generic;
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

        public static IEnumerable<object[]> Integers => new List<object[]>
        {
            new object[] { -5 },
            new object[] { 0 },
            new object[] { 5 },
        };

        public static IEnumerable<object[]> Singles => new List<object[]>
        {
            new object[] { float.NaN },
            new object[] { float.NegativeInfinity },
            new object[] { float.MinValue },
            new object[] { 0.0f },
            new object[] { float.MaxValue },
            new object[] { float.PositiveInfinity },
        };

        [Theory]
        [MemberData(nameof(Integers))]
        [MemberData(nameof(Singles))]
        public void DynamicFloatValues(dynamic value)
        {
            var schema = new FloatSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal((float)value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Integers))]
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
        [MemberData(nameof(Singles))]
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
