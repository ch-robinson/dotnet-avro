namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using System.Xml;
    using Chr.Avro.Abstract;
    using Xunit;

    public class StringSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public StringSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }
#if NET6_0_OR_GREATER

        public static IEnumerable<object[]> DateOnlys => new List<object[]>
        {
            new object[] { DateOnly.MinValue },
            new object[] { new DateOnly(1970, 01, 01) },
            new object[] { DateOnly.MaxValue },
        };
#endif

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

        public static IEnumerable<object[]> Enums => new List<object[]>
        {
            new object[] { DateTimeKind.Unspecified },
            new object[] { DateTimeKind.Local },
        };

        public static IEnumerable<object[]> Guids => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("9281c70a-a916-4bad-9713-936442d7c0e8") },
        };

        public static IEnumerable<object[]> Strings => new List<object[]>
        {
            new object[] { string.Empty },
            new object[] { "12" },
            new object[] { "wizard" },
            new object[] { "🧙" },
        };
#if NET6_0_OR_GREATER

        public static IEnumerable<object[]> TimeOnlys => new List<object[]>
        {
            new object[] { TimeOnly.MinValue },
            new object[] { TimeOnly.MaxValue },
        };
#endif

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
#if NET6_0_OR_GREATER

        [Theory]
        [MemberData(nameof(DateOnlys))]
        public void DynamicDateOnlyValues(dynamic value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value.ToString("O"), deserialize(ref reader));
        }
#endif

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void DynamicDateTimeValues(dynamic value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value.ToString("O"), deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        [MemberData(nameof(Strings))]
        public void DynamicStringValues(dynamic value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value.ToString(), deserialize(ref reader));
        }
#if NET6_0_OR_GREATER

        [Theory]
        [MemberData(nameof(TimeOnlys))]
        public void DynamicTimeOnlyValues(dynamic value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value.ToString("O"), deserialize(ref reader));
        }
#endif

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void DynamicTimeSpanValues(TimeSpan value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(XmlConvert.ToString(value), deserialize(ref reader));
        }
#if NET6_0_OR_GREATER

        [Theory]
        [MemberData(nameof(DateOnlys))]
        public void DateOnlyValues(DateOnly value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateOnly>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateOnlys))]
        public void NullableDateOnlyValues(DateOnly value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateOnly?>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateOnly>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
#endif

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void DateTimeValues(DateTime value)
        {
            var schema = new StringSchema();

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
        public void NullableDateTimeValues(DateTime value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateTime?>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTime>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimeOffsets))]
        public void DateTimeOffsetValues(DateTimeOffset value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeOffset>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
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

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeOffset?>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeOffset>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var decoded = deserialize(ref reader);

            // comparing two DateTimeOffsets doesn’t necessarily ensure that they’re identical:
            Assert.Equal(value.DateTime, decoded.Value.DateTime);
            Assert.Equal(value.Offset, decoded.Value.Offset);
        }

        [Theory]
        [MemberData(nameof(Enums))]
        public void EnumValues(DateTimeKind value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeKind>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeKind>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void GuidValues(Guid value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<Guid>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void NullableGuidValues(Guid value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<Guid?>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Strings))]
        public void StringValues(string value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
#if NET6_0_OR_GREATER

        [Theory]
        [MemberData(nameof(TimeOnlys))]
        public void TimeOnlyValues(TimeOnly value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<TimeOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeOnly>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(TimeOnlys))]
        public void NullableTimeOnlyValues(TimeOnly value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<TimeOnly?>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeOnly>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
#endif

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void TimeSpanValues(TimeSpan value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<TimeSpan>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void NullableTimeSpanValues(TimeSpan value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<TimeSpan?>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Uris))]
        public void UriValues(Uri value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<Uri>(schema);
            var serialize = serializerBuilder.BuildDelegate<Uri>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
