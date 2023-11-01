#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class DateSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public DateSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> DateOnlys => new List<object[]>
        {
            new object[] { new DateOnly(1969, 12, 31) },
            new object[] { new DateOnly(1970, 1, 1) },
            new object[] { new DateOnly(1970, 1, 2) },
        };

        [Theory]
        [MemberData(nameof(DateOnlys))]
        public void DateLogicalTypeToDateOnlyType(DateOnly value)
        {
            var schema = new IntSchema()
            {
                LogicalType = new DateLogicalType(),
            };

            var deserialize = deserializerBuilder.BuildDelegate<DateOnly>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateOnly>(schema);

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
