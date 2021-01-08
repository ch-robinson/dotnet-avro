using Chr.Avro.Abstract;
using System;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class EnumSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public EnumSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [InlineData(Suit.Clubs)]
        [InlineData(Suit.Diamonds)]
        [InlineData(Suit.Hearts)]
        [InlineData(Suit.Spades)]
        public void EnumValues(Suit value)
        {
            var schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES" });

            var deserialize = _deserializerBuilder.BuildDelegate<Suit>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Suit>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void MissingValues()
        {
            var schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES", "EAGLES" });
            Assert.Throws<UnsupportedTypeException>(() => _deserializerBuilder.BuildDelegate<Suit>(schema));

            schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS" });
            Assert.Throws<UnsupportedTypeException>(() => _serializerBuilder.BuildDelegate<Suit>(schema));

            schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES" });
            var serialize = _serializerBuilder.BuildDelegate<Suit>(schema);

            using (_stream)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => serialize((Suit)(-1), new BinaryWriter(_stream)));
            }
        }

        [Theory]
        [InlineData(Suit.Clubs)]
        [InlineData(Suit.Diamonds)]
        [InlineData(Suit.Hearts)]
        [InlineData(Suit.Spades)]
        public void NullableEnumValues(Suit value)
        {
            var schema = new EnumSchema("suit", new[] { "CLUBS", "DIAMONDS", "HEARTS", "SPADES" });

            var deserialize = _deserializerBuilder.BuildDelegate<Suit?>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Suit>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
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
