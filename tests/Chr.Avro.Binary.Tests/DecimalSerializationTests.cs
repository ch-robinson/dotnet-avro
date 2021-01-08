using Chr.Avro.Abstract;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DecimalSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public DecimalSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
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
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ResizedDecimals))]
        public void ResizedDecimalValues(int precision, int scale, decimal value, byte[] encoding, decimal resizing)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(precision, scale)
            };

            var deserialize = _deserializerBuilder.BuildDelegate<decimal>(schema);
            var serialize = _serializerBuilder.BuildDelegate<decimal>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
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
            new object[] { 6, 0, -32769m, new byte[] { 0x06, 0xff, 0x7f, 0xff }, -32769m },
            new object[] { 6, 0, -32768m, new byte[] { 0x04, 0x80, 0x00 }, -32768m },
            new object[] { 5, 2, -1666.6666m, new byte[] { 0x06, 0xfd, 0x74, 0xf6 }, -1666.66m },
            new object[] { 4, 0, -1m, new byte[] { 0x02, 0xff }, -1m },
            new object[] { 6, 5, -0.125, new byte[] { 0x04, 0xcf, 0x2c }, -0.12500 },
            new object[] { 2, 0, -0m, new byte[] { 0x02, 0x00 }, 0m },
            new object[] { 6, 5, 0.125, new byte[] { 0x04, 0x30, 0xd4 }, 0.12500 },
            new object[] { 4, 0, 1m, new byte[] { 0x02, 0x01 }, 1m },
            new object[] { 5, 2, 1666.6666m, new byte[] { 0x06, 0x02, 0x8b, 0x0a }, 1666.66m },
            new object[] { 6, 0, 32767m, new byte[] { 0x04, 0x7f, 0xff }, 32767m },
            new object[] { 6, 0, 32768m, new byte[] { 0x06, 0x00, 0x80, 0x00 }, 32768m },
        };
    }
}
