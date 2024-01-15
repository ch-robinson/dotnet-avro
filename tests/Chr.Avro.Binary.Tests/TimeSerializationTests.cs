#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class TimeSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public TimeSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
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
        [MemberData(nameof(MillisecondTimeEncodings))]
        public void MillisecondTimeLogicalTypeToTimeOnlyType(TimeOnly value, byte[] encoding)
        {
            var schema = new IntSchema()
            {
                LogicalType = new MillisecondTimeLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<TimeOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeOnly>(schema);

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
#endif
