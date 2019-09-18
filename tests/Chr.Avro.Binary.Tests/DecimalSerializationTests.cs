using Chr.Avro.Abstract;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DecimalSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public DecimalSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [MemberData(nameof(Encodings))]
        public void EncodedDecimalValues(int precision, int scale, decimal value, byte[] encoding, decimal resizing)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(precision, scale)
            };

            var serializer = SerializerBuilder.BuildSerializer<decimal>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<decimal>(schema);
            Assert.Equal(resizing, deserializer.Deserialize(encoding));
        }

        public static IEnumerable<object[]> Encodings => new List<object[]>
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
