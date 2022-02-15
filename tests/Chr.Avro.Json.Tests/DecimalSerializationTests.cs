namespace Chr.Avro.Serialization.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class DecimalSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DecimalSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> BoundaryDecimals => new List<object[]>
        {
            new object[] { decimal.MinValue },
            new object[] { decimal.Zero },
            new object[] { decimal.MaxValue },
        };

        public static IEnumerable<object[]> ResizedDecimals => new List<object[]>
        {
            new object[] { 6, 0, -32769m, -32769m },
            new object[] { 6, 0, -32768m, -32768m },
            new object[] { 5, 2, -1666.6666m, -1666.66m },
            new object[] { 4, 0, -1m, -1m },
            new object[] { 6, 5, -0.125, -0.12500 },
            new object[] { 2, 0, -0m, 0m },
            new object[] { 6, 5, 0.125, 0.12500 },
            new object[] { 4, 0, 1m, 1m },
            new object[] { 5, 2, 1666.6666m, 1666.66m },
            new object[] { 6, 0, 32767m, 32767m },
            new object[] { 6, 0, 32768m, 32768m },
        };

        [Theory]
        [MemberData(nameof(BoundaryDecimals))]
        public void BoundaryDecimalValues(decimal value)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(29, 14),
            };

            var deserialize = deserializerBuilder.BuildDelegate<decimal>(schema);
            var serialize = serializerBuilder.BuildDelegate<decimal>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(BoundaryDecimals))]
        public void DynamicDecimalValues(decimal value)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(29, 14),
            };

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ResizedDecimals))]
        public void ResizedDecimalValues(int precision, int scale, decimal value, decimal resizing)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(precision, scale),
            };

            var deserialize = deserializerBuilder.BuildDelegate<decimal>(schema);
            var serialize = serializerBuilder.BuildDelegate<decimal>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(resizing, deserialize(ref reader));
        }
    }
}
