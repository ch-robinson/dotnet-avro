namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Xunit;

    public class EnumSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public EnumSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        [Theory]
        [InlineData(ImplicitEnum.None)]
        [InlineData(ImplicitEnum.First)]
        [InlineData(ImplicitEnum.Second)]
        [InlineData(ImplicitEnum.Third)]
        [InlineData(ImplicitEnum.Fourth)]
        public void EnumValues(ImplicitEnum value)
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH" });

            var deserialize = deserializerBuilder.BuildDelegate<ImplicitEnum>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImplicitEnum>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void MissingValues()
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH", "FIFTH" });
            Assert.Throws<UnsupportedTypeException>(() => deserializerBuilder.BuildDelegate<ImplicitEnum>(schema));

            schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND" });
            Assert.Throws<UnsupportedTypeException>(() => serializerBuilder.BuildDelegate<ImplicitEnum>(schema));

            schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH" });
            var serialize = serializerBuilder.BuildDelegate<ImplicitEnum>(schema);

            using (stream)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => serialize((ImplicitEnum)(-1), new Utf8JsonWriter(stream)));
            }
        }

        [Theory]
        [InlineData(ImplicitEnum.None)]
        [InlineData(ImplicitEnum.First)]
        [InlineData(ImplicitEnum.Second)]
        [InlineData(ImplicitEnum.Third)]
        [InlineData(ImplicitEnum.Fourth)]
        public void NullableEnumValues(ImplicitEnum value)
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH" });

            var deserialize = deserializerBuilder.BuildDelegate<ImplicitEnum?>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImplicitEnum>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
