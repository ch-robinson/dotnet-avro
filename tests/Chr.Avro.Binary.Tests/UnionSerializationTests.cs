using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class UnionSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public UnionSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Fact]
        public void EmptyUnionToObjectType()
        {
            var schema = new UnionSchema();

            Assert.Throws<AggregateException>(() => SerializerBuilder.BuildSerializer<object>(schema));
            Assert.Throws<AggregateException>(() => DeserializerBuilder.BuildDeserializer<object>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntUnionEncodings))]
        public void NullAndIntUnionToInt32Type(int? value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<int>(schema);

            if (value.HasValue)
            {
                Assert.Equal(encoding, serializer.Serialize(value.Value));
            }

            Assert.Throws<AggregateException>(() => DeserializerBuilder.BuildDeserializer<int>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndIntUnionEncodings))]
        public void NullAndIntUnionToNullableInt32Type(int? value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<int?>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<int?>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Fact]
        public void NullAndIntUnionToStringType()
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new IntSchema()
            });

            Assert.Throws<AggregateException>(() => SerializerBuilder.BuildSerializer<string>(schema));
            Assert.Throws<AggregateException>(() => DeserializerBuilder.BuildDeserializer<string>(schema));
        }

        [Theory]
        [MemberData(nameof(NullAndStringUnionEncodings))]
        public void NullAndStringUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema(),
                new StringSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<string>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Theory]
        [MemberData(nameof(NullUnionEncodings))]
        public void NullUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new NullSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<string>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        [Theory]
        [MemberData(nameof(StringUnionEncodings))]
        public void StringUnionToStringType(string value, byte[] encoding)
        {
            var schema = new UnionSchema(new Schema[]
            {
                new StringSchema()
            });

            var serializer = SerializerBuilder.BuildSerializer<string>(schema);
            Assert.Equal(encoding, serializer.Serialize(value));

            var deserializer = DeserializerBuilder.BuildDeserializer<string>(schema);
            Assert.Equal(value, deserializer.Deserialize(encoding));
        }

        public static IEnumerable<object[]> NullAndIntUnionEncodings => new List<object[]>
        {
            new object[] { null, new byte[] { 0x00 } },
            new object[] { 2, new byte[] { 0x02, 0x04 } },
        };

        public static IEnumerable<object[]> NullAndStringUnionEncodings => new List<object[]>
        {
            new object[] { null, new byte[] { 0x00 } },
            new object[] { "test", new byte[] { 0x02, 0x08, 0x74, 0x65, 0x73, 0x74 } },
        };

        public static IEnumerable<object[]> NullUnionEncodings => new List<object[]>
        {
            new object[] { null, new byte[] { 0x00 } },
        };

        public static IEnumerable<object[]> StringUnionEncodings => new List<object[]>
        {
            new object[] { "test", new byte[] { 0x00, 0x08, 0x74, 0x65, 0x73, 0x74 } },
        };
    }
}
