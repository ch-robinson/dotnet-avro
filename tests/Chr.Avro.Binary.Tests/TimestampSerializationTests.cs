using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class TimestampSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public TimestampSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [MemberData(nameof(MicrosecondDateTimeEncodings))]
        public void MicrosecondTimestampLogicalTypeToDateTimeType(DateTime value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTime>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
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
                LogicalType = new MicrosecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
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
                LogicalType = new MillisecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTime>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
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
                LogicalType = new MillisecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }

        public static IEnumerable<object[]> MicrosecondDateTimeEncodings => new List<object[]>
        {
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
                new byte[] { 0x00 }
            },
            new object[]
            {
                new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
                new byte[] { 0xcf, 0x0f }
            },
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
                new byte[] { 0xd0, 0x0f }
            },
        };

        public static IEnumerable<object[]> MillisecondDateTimeEncodings => new List<object[]>
        {
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
                new byte[] { 0x00 }
            },
            new object[]
            {
                new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
                new byte[] { 0x01 }
            },
            new object[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
                new byte[] { 0x02 }
            },
        };
    }
}
