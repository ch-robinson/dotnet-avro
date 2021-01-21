using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class StringSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public StringSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void DateTimeValues(DateTime value)
        {
            var schema = new StringSchema();

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
        public void NullableDateTimeValues(DateTime value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<DateTime?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTime>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimeOffsets))]
        public void DateTimeOffsetValues(DateTimeOffset value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());
            var decoded = deserialize(ref reader);

            // comparing two DateTimeOffsets doesn’t necessarily ensure that they’re identical:
            Assert.Equal(value.DateTime, decoded.DateTime);
            Assert.Equal(value.Offset, decoded.Offset);
        }

        [Theory]
        [MemberData(nameof(DateTimeOffsets))]
        public void NullableDateTimeOffsetValues(DateTimeOffset value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<DateTimeOffset?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());
            var decoded = deserialize(ref reader);

            // comparing two DateTimeOffsets doesn’t necessarily ensure that they’re identical:
            Assert.Equal(value.DateTime, decoded.Value.DateTime);
            Assert.Equal(value.Offset, decoded.Value.Offset);
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void GuidValues(Guid value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<Guid>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Guid>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void NullableGuidValues(Guid value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<Guid?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Guid>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData("")]
        [InlineData("12")]
        [InlineData("wizard")]
        [InlineData("🧙")]
        public void StringValues(string value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void TimeSpanValues(TimeSpan value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<TimeSpan>(schema);
            var serialize = _serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void NullableTimeSpanValues(TimeSpan value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<TimeSpan?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Uris))]
        public void UriValues(Uri value)
        {
            var schema = new StringSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<Uri>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Uri>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        public static IEnumerable<object[]> DateTimes => new List<object[]>
        {
            new object[] { new DateTime(2000, 01, 01, 0, 0, 0, DateTimeKind.Local) },
            new object[] { new DateTime(2000, 01, 01, 0, 0, 0, DateTimeKind.Unspecified) },
            new object[] { new DateTime(2000, 01, 01, 0, 0, 0, DateTimeKind.Utc) },
        };

        public static IEnumerable<object[]> DateTimeOffsets => new List<object[]>
        {
            new object[] { new DateTimeOffset(new DateTime(2000, 01, 01, 0, 0, 0, DateTimeKind.Local)) },
            new object[] { new DateTimeOffset(new DateTime(2000, 01, 01, 0, 0, 0, DateTimeKind.Utc)) },
        };

        public static IEnumerable<object[]> Guids => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("9281c70a-a916-4bad-9713-936442d7c0e8") },
        };

        public static IEnumerable<object[]> TimeSpans => new List<object[]>
        {
            new object[] { TimeSpan.MinValue },
            new object[] { TimeSpan.Zero },
            new object[] { TimeSpan.MaxValue },
        };

        public static IEnumerable<object[]> Uris => new List<object[]>
        {
            new object[] { new Uri("host:443") },
            new object[] { new Uri("https://host") },
            new object[] { new Uri("https://host/") },
            new object[] { new Uri("https://host:443") },
            new object[] { new Uri("https://host/path") },
            new object[] { new Uri("https://host/path?a=query") },
        };
    }
}
