namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class StringSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public StringSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
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

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void DateTimeValues(DateTime value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateTime>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTime>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
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

            var deserialize = deserializerBuilder.BuildDelegate<Guid>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void TimeSpanValues(TimeSpan value)
        {
            var schema = new StringSchema();

            var deserialize = deserializerBuilder.BuildDelegate<TimeSpan>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
