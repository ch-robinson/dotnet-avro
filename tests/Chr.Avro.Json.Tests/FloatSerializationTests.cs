namespace Chr.Avro.Serialization.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class FloatSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public FloatSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
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
            new object[] { float.MinValue },
            new object[] { 0.0f },
            new object[] { float.MaxValue },
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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
