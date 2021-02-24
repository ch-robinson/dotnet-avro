namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Collections.ObjectModel;
    using System.IO;
    using System.Linq;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class ArraySerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public ArraySerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> ArrayData => new List<object[]>
        {
            new object[] { Array.Empty<int>() },
            new object[] { new int[] { -10 } },
            new object[] { new int[] { -10, 10, -5, 5, 0 } },
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
                },
            },
        };

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ArrayValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<int[]>(schema);
            var serialize = serializerBuilder.BuildDelegate<int[]>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ArraySegmentValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ArraySegment<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ArraySegment<int>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void CollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<Collection<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<Collection<int>>(schema);

            using (stream)
            {
                serialize(new Collection<int>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void HashSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<HashSet<string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<HashSet<string>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ICollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ICollection<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ICollection<int>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IEnumerableValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IEnumerable<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IEnumerable<int>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableList<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableList<int>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableList(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableQueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableQueue<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableQueue<int>>(schema);

            using (stream)
            {
                serialize(ImmutableQueue.CreateRange(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void IImmutableSetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableSet<string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableSet<string>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableHashSet(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableStackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableStack<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableStack<int>>(schema);

            using (stream)
            {
                serialize(ImmutableStack.CreateRange(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IList<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IList<int>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyCollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IReadOnlyCollection<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IReadOnlyCollection<int>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IReadOnlyList<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IReadOnlyList<int>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ISetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ISet<string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ISet<string>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableArrayValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableArray<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableArray<int>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableArray(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ImmutableHashSetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableHashSet<string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableHashSet<string>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableHashSet(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableList<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableList<int>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableList(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableQueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableQueue<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableQueue<int>>(schema);

            using (stream)
            {
                serialize(ImmutableQueue.CreateRange(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ImmutableSortedSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableSortedSet<string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableSortedSet<string>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableSortedSet(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableStackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableStack<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableStack<int>>(schema);

            using (stream)
            {
                serialize(ImmutableStack.CreateRange(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(JaggedArrayData))]
        public void JaggedArrayValues(string[][] value)
        {
            var schema = new ArraySchema(new ArraySchema(new StringSchema()));

            var deserialize = deserializerBuilder.BuildDelegate<string[][]>(schema);
            var serialize = serializerBuilder.BuildDelegate<string[][]>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void LinkedListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<LinkedList<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<LinkedList<int>>(schema);

            using (stream)
            {
                serialize(new LinkedList<int>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<List<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<List<int>>(schema);

            using (stream)
            {
                serialize(value.ToList(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void QueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<Queue<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<Queue<int>>(schema);

            using (stream)
            {
                serialize(new Queue<int>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void SortedSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<SortedSet<string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<SortedSet<string>>(schema);

            using (stream)
            {
                serialize(new SortedSet<string>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void StackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = deserializerBuilder.BuildDelegate<Stack<int>>(schema);
            var serialize = serializerBuilder.BuildDelegate<Stack<int>>(schema);

            using (stream)
            {
                serialize(new Stack<int>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
