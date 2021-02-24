namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;
    using Xunit.Sdk;

    public class IntegerSerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public IntegerSerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        [Theory]
        [InlineData(byte.MinValue)]
        [InlineData(byte.MaxValue)]
        public void ByteValues(byte value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<byte>(schema);
            var serialize = serializerBuilder.BuildDelegate<byte>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(DateTimeKind.Unspecified)]
        [InlineData(DateTimeKind.Utc)]
        [InlineData((DateTimeKind)(-1))]
        public void EnumValues(DateTimeKind value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeKind>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeKind>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(short.MinValue)]
        [InlineData(0)]
        [InlineData(short.MaxValue)]
        public void Int16Values(short value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<short>(schema);
            var serialize = serializerBuilder.BuildDelegate<short>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(int.MinValue)]
        [InlineData(0)]
        [InlineData(int.MaxValue)]
        public void Int32Values(int value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(long.MinValue)]
        [InlineData(0)]
        [InlineData(long.MaxValue)]
        public void Int64Values(long value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<long>(schema);
            var serialize = serializerBuilder.BuildDelegate<long>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void OverflowValues()
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = serializerBuilder.BuildDelegate<ulong>(schema);

            using (stream)
            {
                Assert.Throws<OverflowException>(() => serialize(ulong.MaxValue, new Utf8JsonWriter(stream)));

                serialize((ulong)int.MaxValue + 1, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            try
            {
                deserialize(ref reader);

                // since the reader is a ref struct, can't do Assert.Throws
                throw new ThrowsException(typeof(OverflowException));
            }
            catch (OverflowException)
            {
            }
        }

        [Theory]
        [InlineData(sbyte.MinValue)]
        [InlineData(0)]
        [InlineData(sbyte.MaxValue)]
        public void SByteValues(sbyte value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<sbyte>(schema);
            var serialize = serializerBuilder.BuildDelegate<sbyte>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(ushort.MinValue)]
        [InlineData(ushort.MaxValue)]
        public void UInt16Values(ushort value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<ushort>(schema);
            var serialize = serializerBuilder.BuildDelegate<ushort>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(uint.MinValue)]
        [InlineData(uint.MaxValue)]
        public void UInt32Values(uint value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<uint>(schema);
            var serialize = serializerBuilder.BuildDelegate<uint>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(ulong.MinValue)]
        [InlineData(ulong.MaxValue / 2)]
        public void UInt64Values(ulong value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<ulong>(schema);
            var serialize = serializerBuilder.BuildDelegate<ulong>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
