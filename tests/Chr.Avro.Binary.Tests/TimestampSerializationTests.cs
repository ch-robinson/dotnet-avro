using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class TimestampSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public TimestampSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [MemberData(nameof(MicrosecondDateTimeEncodings))]
        public void MicrosecondTimestampLogicalTypeToDateTimeType(DateTime value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType()
            };

            var serializer = SerializerBuilder.BuildSerializer<DateTime>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<DateTime>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Theory]
        [MemberData(nameof(MicrosecondDateTimeEncodings))]
        public void MicrosecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType()
            };

            var serializer = SerializerBuilder.BuildSerializer<DateTimeOffset>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<DateTimeOffset>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Theory]
        [MemberData(nameof(MillisecondDateTimeEncodings))]
        public void MillisecondTimestampLogicalTypeToDateTimeType(DateTime value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType()
            };

            var serializer = SerializerBuilder.BuildSerializer<DateTime>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<DateTime>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Theory]
        [MemberData(nameof(MillisecondDateTimeEncodings))]
        public void MillisecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType()
            };

            var serializer = SerializerBuilder.BuildSerializer<DateTimeOffset>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<DateTimeOffset>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
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
