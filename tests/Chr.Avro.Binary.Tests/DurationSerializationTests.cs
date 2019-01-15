using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DurationSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public DurationSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Fact]
        public void NegativeTimeSpanValues()
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType()
            };

            var serializer = SerializerBuilder.BuildSerializer<TimeSpan>(schema);
            Assert.Throws<OverflowException>(() => serializer.Serialize(TimeSpan.FromMilliseconds(-1)));
        }

        [Theory]
        [MemberData(nameof(TimeSpanEncodings))]
        public void TimeSpanValues(TimeSpan value, byte[] encoding)
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType()
            };

            var serializer = SerializerBuilder.BuildSerializer<TimeSpan>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<TimeSpan>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        public static IEnumerable<object[]> TimeSpanEncodings => new List<object[]>
        {
            new object[]
            {
                TimeSpan.Zero,
                new byte[]
                {
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                }
            },
            new object[]
            {
                new TimeSpan(0, 0, 0, 0, 1),
                new byte[]
                {
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x01, 0x00, 0x00, 0x00,
                }
            },
            new object[]
            {
                new TimeSpan(1, 0, 0, 0),
                new byte[]
                {
                    0x00, 0x00, 0x00, 0x00,
                    0x01, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                }
            },
            new object[]
            {
                TimeSpan.MaxValue.Subtract(
                    TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks % TimeSpan.TicksPerMillisecond)
                ),
                new byte[]
                {
                    0x00, 0x00, 0x00, 0x00,
                    0xff, 0xe3, 0xa2, 0x00,
                    0x65, 0xe4, 0x99, 0x00,
                }
            }
        };
    }
}
