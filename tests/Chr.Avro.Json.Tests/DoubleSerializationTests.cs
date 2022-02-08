namespace Chr.Avro.Serialization.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class DoubleSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DoubleSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> Doubles => new List<object[]>
        {
            new object[] { double.MinValue },
            new object[] { 0.0 },
            new object[] { double.MaxValue },
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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Doubles))]
        public void DynamicDoubleValues(double value)
        {
            var schema = new DoubleSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
