using Chr.Avro.Abstract;
using System;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class IntegerSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public IntegerSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [InlineData(byte.MinValue)]
        [InlineData(byte.MaxValue)]
        public void ByteValues(byte value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<byte>(schema);
            var serializer = SerializerBuilder.BuildSerializer<byte>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(DateTimeKind.Unspecified)]
        [InlineData(DateTimeKind.Utc)]
        [InlineData((DateTimeKind)(-1))]
        public void EnumValues(DateTimeKind value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<DateTimeKind>(schema);
            var serializer = SerializerBuilder.BuildSerializer<DateTimeKind>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(short.MinValue)]
        [InlineData(0)]
        [InlineData(short.MaxValue)]
        public void Int16Values(short value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<short>(schema);
            var serializer = SerializerBuilder.BuildSerializer<short>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(int.MinValue)]
        [InlineData(0)]
        [InlineData(int.MaxValue)]
        public void Int32Values(int value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<int>(schema);
            var serializer = SerializerBuilder.BuildSerializer<int>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(long.MinValue)]
        [InlineData(0)]
        [InlineData(long.MaxValue)]
        public void Int64Values(long value)
        {
            var schema = new LongSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<long>(schema);
            var serializer = SerializerBuilder.BuildSerializer<long>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Fact]
        public void OverflowValues()
        {
            var schema = new LongSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<int>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ulong>(schema);

            Assert.Throws<OverflowException>(() => serializer.Serialize(ulong.MaxValue));
            Assert.Throws<OverflowException>(() => deserializer.Deserialize(serializer.Serialize((ulong)int.MaxValue + 1)));
        }
        
        [Theory]
        [InlineData(sbyte.MinValue)]
        [InlineData(0)]
        [InlineData(sbyte.MaxValue)]
        public void SByteValues(sbyte value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<sbyte>(schema);
            var serializer = SerializerBuilder.BuildSerializer<sbyte>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(ushort.MinValue)]
        [InlineData(ushort.MaxValue)]
        public void UInt16Values(ushort value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<ushort>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ushort>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(uint.MinValue)]
        [InlineData(uint.MaxValue)]
        public void UInt32Values(uint value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<uint>(schema);
            var serializer = SerializerBuilder.BuildSerializer<uint>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [InlineData(ulong.MinValue)]
        [InlineData(ulong.MaxValue / 2)]
        public void UInt64Values(ulong value)
        {
            var schema = new IntSchema();

            var deserializer = DeserializerBuilder.BuildDeserializer<ulong>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ulong>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }
    }
}
