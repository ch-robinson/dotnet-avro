using System;
using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class EnumSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public EnumSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(Suit.Clubs)]
        [InlineData(Suit.Diamonds)]
        [InlineData(Suit.Hearts)]
        [InlineData(Suit.Spades)]
        public void EnumValues(Suit value)
        {
            var schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES" });

            var deserializer = DeserializerBuilder.BuildDeserializer<Suit>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Suit>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Fact]
        public void MissingValues()
        {
            var schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES", "EAGLES" });
            Assert.Throws<UnsupportedTypeException>(() => DeserializerBuilder.BuildDeserializer<Suit>(schema));

            schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS" });
            Assert.Throws<UnsupportedTypeException>(() => SerializerBuilder.BuildSerializer<Suit>(schema));

            schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES" });
            var serializer = SerializerBuilder.BuildSerializer<Suit>(schema);
            Assert.Throws<ArgumentOutOfRangeException>(() => serializer.Serialize((Suit)(-1)));
        }

        [Theory]
        [InlineData(Suit.Clubs)]
        [InlineData(Suit.Diamonds)]
        [InlineData(Suit.Hearts)]
        [InlineData(Suit.Spades)]
        public void NullableEnumValues(Suit value)
        {
            var schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES" });

            var deserializer = DeserializerBuilder.BuildDeserializer<Suit?>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Suit>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        public enum Suit
        {
            Clubs,
            Diamonds,
            Hearts,
            Spades
        }
    }
}
