#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class TimeSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly TestBufferWriter bufferWriter;

        public TimeSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            bufferWriter = new TestBufferWriter();
        }

        public static IEnumerable<object[]> MicrosecondTimeEncodings => new List<object[]>
        {
            new object[]
            {
                new TimeOnly(0, 0, 0, 0),
                new byte[] { 0x00 },
            },
            new object[]
            {
                new TimeOnly(0, 0, 0, 1),
                new byte[] { 0xd0, 0x0f },
            },
        };

        public static IEnumerable<object[]> MillisecondTimeEncodings => new List<object[]>
        {
            new object[]
            {
                new TimeOnly(0, 0, 0, 0),
                new byte[] { 0x00 },
            },
            new object[]
            {
                new TimeOnly(0, 0, 0, 1),
                new byte[] { 0x02 },
            },
        };

        [Theory]
        [MemberData(nameof(MicrosecondTimeEncodings))]
        public void MicrosecondTimeLogicalTypeToTimeOnlyType(TimeOnly value, byte[] encoding)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimeLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<TimeOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeOnly>(schema);

            serialize(value, new BinaryWriter(bufferWriter));

            var encoded = bufferWriter.WrittenSpan.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(MillisecondTimeEncodings))]
        public void MillisecondTimeLogicalTypeToTimeOnlyType(TimeOnly value, byte[] encoding)
        {
            var schema = new IntSchema()
            {
                LogicalType = new MillisecondTimeLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<TimeOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeOnly>(schema);

            serialize(value, new BinaryWriter(bufferWriter));

            var encoded = bufferWriter.WrittenSpan.ToArray();
            var reader = new BinaryReader(encoded);

            Assert.Equal(encoding, encoded);
            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
#endif
