namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.IO;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class EnumSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public EnumSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
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

            using (stream)
            {
                serialize("FIFTH", new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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

            using (stream)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => serialize((ImplicitEnum)(-1), new BinaryWriter(stream)));
            }
        }

        [Fact]
        public void MissingStringValues()
        {
            var schema = new EnumSchema("ordinal", new[] { "NONE", "FIRST" });
            var serialize = serializerBuilder.BuildDelegate<string>(schema);

            using (stream)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => serialize("SECOND", new BinaryWriter(stream)));
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
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
