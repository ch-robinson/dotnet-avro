using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class StringSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public StringSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [MemberData(nameof(DateTimes))]
        public void DateTimeValues(DateTime value)
        {
            var schema = new StringSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<DateTime>(schema);
            var serializer = SerializerBuilder.BuildSerializer<DateTime>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(DateTimeOffsets))]
        public void DateTimeOffsetValues(DateTimeOffset value)
        {
            var schema = new StringSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<DateTimeOffset>(schema);
            var serializer = SerializerBuilder.BuildSerializer<DateTimeOffset>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void GuidValues(Guid value)
        {
            var schema = new StringSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<Guid>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Guid>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData("")]
        [InlineData("12")]
        [InlineData("wizard")]
        [InlineData("🧙")]
        public void StringValues(string value)
        {
            var schema = new StringSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            var serializer = SerializerBuilder.BuildSerializer<string>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void TimeSpanValues(TimeSpan value)
        {
            var schema = new StringSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<TimeSpan>(schema);
            var serializer = SerializerBuilder.BuildSerializer<TimeSpan>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(Uris))]
        public void UriValues(Uri value)
        {
            var schema = new StringSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<Uri>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Uri>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
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
