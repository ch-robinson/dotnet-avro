namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;
    using Xunit.Sdk;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class IntegerSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public IntegerSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> Bytes => new List<object[]>
        {
            new object[] { byte.MinValue },
            new object[] { byte.MaxValue },
        };

        public static IEnumerable<object[]> Enums => new List<object[]>
        {
            new object[] { DateTimeKind.Unspecified },
            new object[] { DateTimeKind.Utc },
            new object[] { (DateTimeKind)(-1) },
        };

        public static IEnumerable<object[]> Int16s => new List<object[]>
        {
            new object[] { short.MinValue },
            new object[] { (short)0 },
            new object[] { short.MaxValue },
        };

        public static IEnumerable<object[]> Int32s => new List<object[]>
        {
            new object[] { int.MinValue },
            new object[] { 0 },
            new object[] { int.MaxValue },
        };

        public static IEnumerable<object[]> Int64s => new List<object[]>
        {
            new object[] { long.MinValue },
            new object[] { 0L },
            new object[] { long.MaxValue },
        };

        public static IEnumerable<object[]> IntPtrs => new List<object[]>
        {
            new object[] { new IntPtr(int.MinValue) },
            new object[] { new IntPtr(0) },
            new object[] { new IntPtr(int.MaxValue) },
        };

        public static IEnumerable<object[]> SBytes => new List<object[]>
        {
            new object[] { sbyte.MinValue },
            new object[] { (sbyte)0 },
            new object[] { sbyte.MaxValue },
        };

        public static IEnumerable<object[]> UInt16s => new List<object[]>
        {
            new object[] { ushort.MinValue },
            new object[] { ushort.MaxValue },
        };

        public static IEnumerable<object[]> UInt32s => new List<object[]>
        {
            new object[] { uint.MinValue },
            new object[] { uint.MaxValue / 2 },
        };

        public static IEnumerable<object[]> UInt64s => new List<object[]>
        {
            new object[] { ulong.MinValue },
            new object[] { ulong.MaxValue / 2 },
        };

        public static IEnumerable<object[]> UIntPtrs => new List<object[]>
        {
            new object[] { new UIntPtr(uint.MinValue) },
            new object[] { new UIntPtr(uint.MaxValue) },
        };

        [Theory]
        [MemberData(nameof(Bytes))]
        public void ByteValues(byte value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<byte>(schema);
            var serialize = serializerBuilder.BuildDelegate<byte>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Bytes))]
        [MemberData(nameof(Enums))]
        [MemberData(nameof(Int16s))]
        [MemberData(nameof(Int32s))]
        [MemberData(nameof(Int64s))]
        [MemberData(nameof(SBytes))]
        [MemberData(nameof(UInt16s))]
        [MemberData(nameof(UInt32s))]
        [MemberData(nameof(UInt64s))]
        public void DynamicLongValues(dynamic value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal((long)value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Enums))]
        public void EnumValues(DateTimeKind value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<DateTimeKind>(schema);
            var serialize = serializerBuilder.BuildDelegate<DateTimeKind>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Int16s))]
        public void Int16Values(short value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<short>(schema);
            var serialize = serializerBuilder.BuildDelegate<short>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Int32s))]
        public void Int32Values(int value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<int>(schema);
            var serialize = serializerBuilder.BuildDelegate<int>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Int64s))]
        public void Int64Values(long value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<long>(schema);
            var serialize = serializerBuilder.BuildDelegate<long>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(IntPtrs))]
        public void IntPtrValues(IntPtr value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<IntPtr>(schema);
            var serialize = serializerBuilder.BuildDelegate<IntPtr>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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
                Assert.Throws<OverflowException>(() => serialize(ulong.MaxValue, new BinaryWriter(stream)));

                serialize((ulong)int.MaxValue + 1, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

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
        [MemberData(nameof(SBytes))]
        public void SByteValues(sbyte value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<sbyte>(schema);
            var serialize = serializerBuilder.BuildDelegate<sbyte>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(UInt16s))]
        public void UInt16Values(ushort value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<ushort>(schema);
            var serialize = serializerBuilder.BuildDelegate<ushort>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(UInt32s))]
        public void UInt32Values(uint value)
        {
            var schema = new IntSchema();

            var deserialize = deserializerBuilder.BuildDelegate<uint>(schema);
            var serialize = serializerBuilder.BuildDelegate<uint>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(UInt64s))]
        public void UInt64Values(ulong value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<ulong>(schema);
            var serialize = serializerBuilder.BuildDelegate<ulong>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(UIntPtrs))]
        public void UIntPtrValues(UIntPtr value)
        {
            var schema = new LongSchema();

            var deserialize = deserializerBuilder.BuildDelegate<UIntPtr>(schema);
            var serialize = serializerBuilder.BuildDelegate<UIntPtr>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
