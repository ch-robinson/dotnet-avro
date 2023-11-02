#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class TimeSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public TimeSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> TimeOnlys => new List<object[]>
        {
            new object[] { new TimeOnly(0, 0, 0, 0) },
            new object[] { new TimeOnly(0, 0, 0, 1) },
        };

        [Theory]
        [MemberData(nameof(TimeOnlys))]
        public void MicrosecondTimeLogicalTypeToTimeOnlyType(TimeOnly value)
        {
            var schema = new LongSchema()
            {
                LogicalType = new MicrosecondTimeLogicalType(),
            };

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
        public void MillisecondTimeLogicalTypeToTimeOnlyType(TimeOnly value)
        {
            var schema = new IntSchema()
            {
                LogicalType = new MillisecondTimeLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<TimeOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<TimeOnly>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
#endif
