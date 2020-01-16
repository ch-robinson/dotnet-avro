using Chr.Avro.Abstract;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class ArraySerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public ArraySerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ArrayValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<int[]>(schema);
            var serializer = SerializerBuilder.BuildSerializer<int[]>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ICollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ICollection<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ICollection<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IEnumerableValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IEnumerable<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IEnumerable<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(ISetData))]
        public void ISetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new NullSchema());

            var serializer = SerializerBuilder.BuildSerializer<ISet<string>>(schema);
            var encoding = serializer.Serialize(value);

            Assert.Throws<UnsupportedTypeException>(() => DeserializerBuilder.BuildDeserializer<ISet<string>>(schema));
        }

        [Theory]
        [MemberData(nameof(JaggedArrayData))]
        public void JaggedArrayValues(string[][] value)
        {
            var schema = new ArraySchema(new ArraySchema(new StringSchema()));

            var deserializer = DeserializerBuilder.BuildDeserializer<string[][]>(schema);
            var serializer = SerializerBuilder.BuildSerializer<string[][]>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<List<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<List<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToList())));
        }

        [Theory]
        [MemberData(nameof(ISetData))]
        public void HashSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());
            var deserializer = DeserializerBuilder.BuildDeserializer<HashSet<string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<HashSet<string>>(schema);

            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        public static IEnumerable<object[]> ArrayData => new List<object[]>
        {
            new object[] { new int[] { } },
            new object[] { new int[] { -10 } },
            new object[] { new int[] { -10, 10, -5, 5, 0} },
        };

        public static IEnumerable<object[]> ISetData => new List<object[]>
        {
            new object[] { new HashSet<string>() },
            new object[] { new HashSet<string>() { "a", "as", "aspen" } },
        };

        public static IEnumerable<object[]> JaggedArrayData => new List<object[]>
        {
            new object[]
            {
                new string[][]
                {
                    new[] { "lawful good", "neutral good", "chaotic good" },
                    new[] { "lawful neutral", "true neutral", "chaotic neutral" },
                    new[] { "lawful evil", "neutral evil", "chaotic evil" },
                }
            },
        };
    }
}
