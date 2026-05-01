namespace Chr.Avro.Serialization.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class EnumSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly TestBufferWriter bufferWriter;

        public EnumSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            bufferWriter = new TestBufferWriter();
        }

        [Fact]
        public void DefaultEnumValues()
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH", "FIFTH" })
            {
                Default = "NONE",
            };

            var deserialize = deserializerBuilder.BuildDelegate<ImplicitEnum>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            serialize("FIFTH", new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(ImplicitEnum.None, deserialize(ref reader));
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

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void MissingEnumValues()
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH", "FIFTH" });
            Assert.Throws<UnsupportedTypeException>(() => deserializerBuilder.BuildDelegate<ImplicitEnum>(schema));

            schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND" });
            Assert.Throws<UnsupportedTypeException>(() => serializerBuilder.BuildDelegate<ImplicitEnum>(schema));

            schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH" });
            var serialize = serializerBuilder.BuildDelegate<ImplicitEnum>(schema);

            Assert.Throws<ArgumentOutOfRangeException>(() => serialize((ImplicitEnum)(-1), new BinaryWriter(bufferWriter)));
        }

        [Fact]
        public void MissingStringValues()
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST" });
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            Assert.Throws<ArgumentOutOfRangeException>(() => serialize("SECOND", new BinaryWriter(bufferWriter)));
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

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData("NONE")]
        [InlineData("FIRST")]
        [InlineData("SECOND")]
        [InlineData("THIRD")]
        [InlineData("FOURTH")]
        public void StringValues(string value)
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST", "SECOND", "THIRD", "FOURTH" });

            var deserialize = deserializerBuilder.BuildDelegate<string>(schema);
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            serialize(value, new BinaryWriter(bufferWriter));

            var reader = new BinaryReader(bufferWriter.WrittenSpan);

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
