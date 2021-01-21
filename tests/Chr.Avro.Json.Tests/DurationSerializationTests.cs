using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DurationSerializationTests
    {
        private readonly IJsonDeserializerBuilder _deserializerBuilder;

        private readonly IJsonSerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public DurationSerializationTests()
        {
            _deserializerBuilder = new JsonDeserializerBuilder();
            _serializerBuilder = new JsonSerializerBuilder();
            _stream = new MemoryStream();
        }

        [Fact]
        public void NegativeTimeSpanValues()
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType()
            };

            var serialize = _serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (_stream)
            {
                Assert.Throws<OverflowException>(() => serialize(TimeSpan.FromMilliseconds(-1), new Utf8JsonWriter(_stream)));
            }
        }

        [Theory]
        [MemberData(nameof(TimeSpans))]
        public void TimeSpanValues(TimeSpan value)
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType()
            };

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
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<TimeSpan?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (_stream)
            {
                serialize(value, new Utf8JsonWriter(_stream));
            }

            var reader = new Utf8JsonReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        public static IEnumerable<object[]> TimeSpans => new List<object[]>
        {
            new object[] { TimeSpan.Zero },
            new object[] { new TimeSpan(0, 0, 0, 0, 1) },
            new object[] { new TimeSpan(1, 0, 0, 0) },
            new object[] { TimeSpan.MaxValue.Subtract(TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks % TimeSpan.TicksPerMillisecond)) }
        };
    }
}
