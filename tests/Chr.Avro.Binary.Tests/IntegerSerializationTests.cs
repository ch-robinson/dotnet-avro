using Chr.Avro.Abstract;
using System;
using System.IO;
using Xunit;
using Xunit.Sdk;

namespace Chr.Avro.Serialization.Tests
{
    public class IntegerSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public IntegerSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [InlineData(byte.MinValue)]
        [InlineData(byte.MaxValue)]
        public void ByteValues(byte value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<byte>(schema);
            var serialize = _serializerBuilder.BuildDelegate<byte>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(DateTimeKind.Unspecified)]
        [InlineData(DateTimeKind.Utc)]
        [InlineData((DateTimeKind)(-1))]
        public void EnumValues(DateTimeKind value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<DateTimeKind>(schema);
            var serialize = _serializerBuilder.BuildDelegate<DateTimeKind>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(short.MinValue)]
        [InlineData(0)]
        [InlineData(short.MaxValue)]
        public void Int16Values(short value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<short>(schema);
            var serialize = _serializerBuilder.BuildDelegate<short>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(int.MinValue)]
        [InlineData(0)]
        [InlineData(int.MaxValue)]
        public void Int32Values(int value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(long.MinValue)]
        [InlineData(0)]
        [InlineData(long.MaxValue)]
        public void Int64Values(long value)
        {
            var schema = new LongSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<long>(schema);
            var serialize = _serializerBuilder.BuildDelegate<long>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Fact]
        public void OverflowValues()
        {
            var schema = new LongSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ulong>(schema);

            using (_stream)
            {
                Assert.Throws<OverflowException>(() => serialize(ulong.MaxValue, new BinaryWriter(_stream)));

                serialize((ulong)int.MaxValue + 1, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            try
            {
                deserialize(ref reader);

                // since the reader is a ref struct, can't do Assert.Throws
                throw new ThrowsException(typeof(OverflowException));
            }
            catch (OverflowException)
            {
                //
            }
        }

        [Theory]
        [InlineData(sbyte.MinValue)]
        [InlineData(0)]
        [InlineData(sbyte.MaxValue)]
        public void SByteValues(sbyte value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<sbyte>(schema);
            var serialize = _serializerBuilder.BuildDelegate<sbyte>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(ushort.MinValue)]
        [InlineData(ushort.MaxValue)]
        public void UInt16Values(ushort value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<ushort>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ushort>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(uint.MinValue)]
        [InlineData(uint.MaxValue)]
        public void UInt32Values(uint value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<uint>(schema);
            var serialize = _serializerBuilder.BuildDelegate<uint>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [InlineData(ulong.MinValue)]
        [InlineData(ulong.MaxValue / 2)]
        public void UInt64Values(ulong value)
        {
            var schema = new IntSchema();

            var deserialize = _deserializerBuilder.BuildDelegate<ulong>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ulong>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
