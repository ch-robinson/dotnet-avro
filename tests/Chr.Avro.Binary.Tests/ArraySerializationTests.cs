using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
        public void ArraySegmentValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ArraySegment<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ArraySegment<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void HashSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<HashSet<string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<HashSet<string>>(schema);
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
        [MemberData(nameof(ArrayData))]
        public void IImmutableListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IImmutableList<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IImmutableList<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableList())));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableQueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IImmutableQueue<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IImmutableQueue<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(ImmutableQueue.CreateRange(value))));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void IImmutableSetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IImmutableSet<string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IImmutableSet<string>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableHashSet())).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableStackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IImmutableStack<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IImmutableStack<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(ImmutableStack.CreateRange(value))));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IList<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IList<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyCollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IReadOnlyCollection<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IReadOnlyCollection<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IReadOnlyList<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IReadOnlyList<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ISetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ISet<string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ISet<string>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableArrayValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableArray<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableArray<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableArray())));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ImmutableHashSetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableHashSet<string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableHashSet<string>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableHashSet())).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableList<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableList<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableList())));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableQueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableQueue<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableQueue<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(ImmutableQueue.CreateRange(value))));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ImmutableSortedSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableSortedSet<string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableSortedSet<string>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableSortedSet())));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableStackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableStack<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableStack<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(ImmutableStack.CreateRange(value))));
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
        public void LinkedListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<LinkedList<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<LinkedList<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(new LinkedList<int>(value))));
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
        [MemberData(nameof(ArrayData))]
        public void QueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<Queue<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Queue<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(new Queue<int>(value))));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void SortedSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<SortedSet<string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<SortedSet<string>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(new SortedSet<string>(value))));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void StackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<Stack<int>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Stack<int>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(new Stack<int>(value))));
        }

        public static IEnumerable<object[]> ArrayData => new List<object[]>
        {
            new object[] { new int[] { } },
            new object[] { new int[] { -10 } },
            new object[] { new int[] { -10, 10, -5, 5, 0} },
        };

        public static IEnumerable<object[]> SetData => new List<object[]>
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
