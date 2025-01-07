namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class TimestampSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public TimestampSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> DateTimes => new List<object[]>
        {
            new object[] { new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc) },
            new object[] { new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
        };

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MicrosecondTimestampLogicalTypeToDateTimeType(DateTime value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTime>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MicrosecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MillisecondTimestampLogicalTypeToDateTimeType(DateTime value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTime>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void MillisecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void NanosecondTimestampLogicalTypeToDateTimeType(DateTime value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new NanosecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTime>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void NanosecondTimestampLogicalTypeToDateTimeOffsetType(DateTimeOffset value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new NanosecondTimestampLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
