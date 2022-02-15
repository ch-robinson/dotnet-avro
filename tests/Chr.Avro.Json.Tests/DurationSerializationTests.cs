namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class DurationSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DurationSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> TimeSpans => new List<object[]>
        {
            new object[] { TimeSpan.Zero },
            new object[] { new TimeSpan(0, 0, 0, 0, 1) },
            new object[] { new TimeSpan(1, 0, 0, 0) },
            new object[] { TimeSpan.MaxValue.Subtract(TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks % TimeSpan.TicksPerMillisecond)) },
        };

        [Fact]
        public void NegativeTimeSpanValues()
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType(),
            };

            var serialize = serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (stream)
            {
                Assert.Throws<OverflowException>(() => serialize(TimeSpan.FromMilliseconds(-1), new Utf8JsonWriter(stream)));
            }
        }

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void TimeSpanValues(TimeSpan value)
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType(),
            };

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
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<TimeSpan?>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
