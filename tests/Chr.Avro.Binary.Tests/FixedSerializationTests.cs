namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class FixedSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public FixedSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> ByteArrays => new List<object[]>
        {
            new object[] { Array.Empty<byte>() },
            new object[] { new byte[] { 0x00 } },
            new object[] { new byte[] { 0xf0, 0x9f, 0x92, 0x81, 0xf0, 0x9f, 0x8e, 0x8d } },
        };

        public static IEnumerable<object[]> Guids => new List<object[]>
        {
            new object[] { Guid.Empty },
            new object[] { Guid.Parse("ed7ba470-8e54-465e-825c-99712043e01c") },
        };

        [Theory]
        [MemberData(nameof(ByteArrays))]
        public void ByteArrayValues(byte[] value)
        {
            var schema = new FixedSchema("test", value.Length);

            var deserialize = deserializerBuilder.BuildDelegate<byte[]>(schema);
            var serialize = serializerBuilder.BuildDelegate<byte[]>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ByteArrays))]
        public void DynamicByteArrayValues(byte[] value)
        {
            var schema = new FixedSchema("test", value.Length);

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void GuidValues(Guid value)
        {
            var schema = new FixedSchema("test", 16);

            var deserialize = deserializerBuilder.BuildDelegate<Guid>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(Guids))]
        public void NullableGuidValues(Guid value)
        {
            var schema = new FixedSchema("test", 16);

            var deserialize = deserializerBuilder.BuildDelegate<Guid?>(schema);
            var serialize = serializerBuilder.BuildDelegate<Guid>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
