namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class DurationSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DurationSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
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
                },
            },
            new object[]
            {
                new TimeSpan(0, 0, 0, 0, 1),
                new byte[]
                {
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x01, 0x00, 0x00, 0x00,
                },
            },
            new object[]
            {
                new TimeSpan(1, 0, 0, 0),
                new byte[]
                {
                    0x00, 0x00, 0x00, 0x00,
                    0x01, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                },
            },
            new object[]
            {
                TimeSpan.MaxValue.Subtract(
                    TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks % TimeSpan.TicksPerMillisecond)),
                new byte[]
                {
                    0x00, 0x00, 0x00, 0x00,
                    0xff, 0xe3, 0xa2, 0x00,
                    0x65, 0xe4, 0x99, 0x00,
                },
            },
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
                Assert.Throws<OverflowException>(() => serialize(TimeSpan.FromMilliseconds(-1), new BinaryWriter(stream)));
            }
        }

        [Theory]
        [MemberData(nameof(TimeSpanEncodings))]
        public void TimeSpanValues(TimeSpan value, byte[] encoding)
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<TimeSpan>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(TimeSpanEncodings))]
        public void NullableTimeSpanValues(TimeSpan value, byte[] encoding)
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<TimeSpan?>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var encoded = stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
