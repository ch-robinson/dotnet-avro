using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class DurationSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public DurationSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
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
                Assert.Throws<OverflowException>(() => serialize(TimeSpan.FromMilliseconds(-1), new BinaryWriter(_stream)));
            }
        }

        [Theory]
        [MemberData(nameof(TimeSpanEncodings))]
        public void TimeSpanValues(TimeSpan value, byte[] encoding)
        {
            var schema = new FixedSchema("duration", DurationLogicalType.DurationSize)
            {
                LogicalType = new DurationLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<TimeSpan>(schema);
            var serialize = _serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
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
                LogicalType = new DurationLogicalType()
            };

            var deserialize = _deserializerBuilder.BuildDelegate<TimeSpan?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<TimeSpan>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var encoded = _stream.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
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
