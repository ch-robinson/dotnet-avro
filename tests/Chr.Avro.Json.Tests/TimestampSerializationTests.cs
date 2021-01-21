using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class TimestampSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public TimestampSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MicrosecondTimestampLogicalTypeToDateTimeType(DateTime value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTime>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MicrosecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MillisecondTimestampLogicalTypeToDateTimeType(DateTime value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTime>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MillisecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        public static IEnumerable<object[]> DateTimes => new List<object[]>
        {
            new object[] { new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc) },
            new object[] { new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
        };
    }
}
