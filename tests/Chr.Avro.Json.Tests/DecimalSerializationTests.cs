using Chr.Avro.Abstract;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DecimalSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public DecimalSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [MemberData(nameof(BoundaryDecimals))]
        public void BoundaryDecimalValues(decimal value)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(29, 14)
            };

            var deserialize = _deserializerBuilder.BuildDelegate<decimal>(schema);
            var serialize = _serializerBuilder.BuildDelegate<decimal>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ResizedDecimals))]
        public void ResizedDecimalValues(int precision, int scale, decimal value, decimal resizing)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(precision, scale)
            };

            var deserialize = _deserializerBuilder.BuildDelegate<decimal>(schema);
            var serialize = _serializerBuilder.BuildDelegate<decimal>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(resizing, deserialize(ref reader));
        }

        public static IEnumerable<object[]> BoundaryDecimals => new List<object[]>
        {
            new object[] { decimal.MinValue },
            new object[] { decimal.Zero },
            new object[] { decimal.MaxValue }
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
    }
}
