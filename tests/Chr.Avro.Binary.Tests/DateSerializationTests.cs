#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class DateSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DateSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> DateEncodings => new List<object[]>
        {
            new object[]
            {
                new DateOnly(1969, 12, 31),
                new byte[] { 0x01 },
            },
            new object[]
            {
                new DateOnly(1970, 1, 1),
                new byte[] { 0x00 },
            },
            new object[]
            {
                new DateOnly(1970, 1, 2),
                new byte[] { 0x02 },
            },
        };

        [Theory]
        [MemberData(nameof(DateEncodings))]
        public void DateLogicalTypeToDateOnlyType(DateOnly value, byte[] encoding)
        {
            var schema = new IntSchema()
            {
                LogicalType = new DateLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateOnly>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
#endif
