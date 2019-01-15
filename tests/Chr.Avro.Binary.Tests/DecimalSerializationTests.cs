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
        [MemberData(nameof(Resizings))]
        public void ResizedDecimalValues(int precision, int scale, decimal value, decimal rounded)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(precision, scale)
            };

            var deserializer = DeserializerBuilder.BuildDeserializer<decimal>(schema);
            var serializer = SerializerBuilder.BuildSerializer<decimal>(schema);

            Assert.Equal(rounded, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(ZeroScaleEncodings))]
        public void ZeroScaleDecimalValues(decimal value, byte[] encoding)
        {
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(12, 0)
            };

            var serializer = SerializerBuilder.BuildSerializer<decimal>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<decimal>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        public static IEnumerable<object[]> Resizings => new List<object[]>
        {
            new object[] { 1, 0, 404.04m, 400m },
            new object[] { 3, 0, 404.04m, 404m },
            new object[] { 5, 0, 404.0404m, 404m },
            new object[] { 5, 2, -404.0m, -404.00m },
            new object[] { 5, 4, -404.0404m, -404.0400m },
        };

        public static IEnumerable<object[]> ZeroScaleEncodings => new List<object[]>
        {
            new object[] { -32768m, new byte[] { 0x04, 0x80, 0x00 } },
            new object[] { -1m, new byte[] { 0x02, 0xff } },
            new object[] { -0m, new byte[] { 0x02, 0x00 } },
            new object[] { 1m, new byte[] { 0x02, 0x01 } },
            new object[] { 32767m, new byte[] { 0x04, 0x7f, 0xff } },
        };
    }
}
