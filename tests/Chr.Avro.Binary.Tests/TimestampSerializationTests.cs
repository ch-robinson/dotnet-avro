namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class TimestampSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public TimestampSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> MicrosecondDateTimeEncodings => new List<object[]>
        {
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
                new byte[] { 0x00 },
            },
            new object[]
            {
                new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
                new byte[] { 0xcf, 0x0f },
            },
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
                new byte[] { 0xd0, 0x0f },
            },
        };

        public static IEnumerable<object[]> MillisecondDateTimeEncodings => new List<object[]>
        {
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
                new byte[] { 0x00 },
            },
            new object[]
            {
                new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
                new byte[] { 0x01 },
            },
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
                new byte[] { 0x02 },
            },
        };

        [Theory]
        [MemberData(nameof(MicrosecondDateTimeEncodings))]
        public void MicrosecondTimestampLogicalTypeToDateTimeType(DateTime value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTime>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(MicrosecondDateTimeEncodings))]
        public void MicrosecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(MillisecondDateTimeEncodings))]
        public void MillisecondTimestampLogicalTypeToDateTimeType(DateTime value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTime>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(MillisecondDateTimeEncodings))]
        public void MillisecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

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
