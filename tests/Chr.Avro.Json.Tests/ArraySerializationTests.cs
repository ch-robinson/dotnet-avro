namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Collections.ObjectModel;
    using System.IO;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class ArraySerializationTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public ArraySerializationTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> ArrayData => new List<object[]>
        {
            new object[] { Array.Empty<long>() },
            new object[] { new long[] { -10 } },
            new object[] { new long[] { -10, 10, -5, 5, 0 } },
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
        public void ArrayValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<long[]>(schema);
            var serialize = serializerBuilder.BuildDelegate<long[]>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ArraySegmentValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ArraySegment<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ArraySegment<long>>(schema);

            using (stream)
            {
                serialize(new ArraySegment<long>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void CollectionValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<Collection<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<Collection<long>>(schema);

            using (stream)
            {
                serialize(new Collection<long>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void DynamicArrayValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value.Cast<object>(), deserialize(ref reader));
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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ICollectionValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ICollection<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ICollection<long>>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IEnumerableValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IEnumerable<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IEnumerable<long>>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableListValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableList<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableList<long>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableList(), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableQueueValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableQueue<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableQueue<long>>(schema);

            using (stream)
            {
                serialize(ImmutableQueue.CreateRange(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value.ToImmutableHashSet(), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableStackValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableStack<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableStack<long>>(schema);

            using (stream)
            {
                serialize(ImmutableStack.CreateRange(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IListValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IList<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IList<long>>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyCollectionValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IReadOnlyCollection<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IReadOnlyCollection<long>>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyListValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IReadOnlyList<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IReadOnlyList<long>>(schema);

            using (stream)
            {
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableArrayValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableArray<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableArray<long>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableArray(), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value.ToImmutableHashSet(), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableListValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableList<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableList<long>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableList(), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableQueueValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableQueue<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableQueue<long>>(schema);

            using (stream)
            {
                serialize(ImmutableQueue.CreateRange(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value.ToImmutableSortedSet(), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableStackValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableStack<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableStack<long>>(schema);

            using (stream)
            {
                serialize(ImmutableStack.CreateRange(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(value, new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void LinkedListValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<LinkedList<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<LinkedList<long>>(schema);

            using (stream)
            {
                serialize(new LinkedList<long>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ListValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<List<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<List<long>>(schema);

            using (stream)
            {
                serialize(value.ToList(), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ObservableCollectionValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ObservableCollection<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ObservableCollection<long>>(schema);

            using (stream)
            {
                serialize(new ObservableCollection<long>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void QueueValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<Queue<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<Queue<long>>(schema);

            using (stream)
            {
                serialize(new Queue<long>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ReadOnlyCollectionValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ReadOnlyCollection<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ReadOnlyCollection<long>>(schema);

            using (stream)
            {
                serialize(new ReadOnlyCollection<long>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ReadOnlyObservableCollectionValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ReadOnlyObservableCollection<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ReadOnlyObservableCollection<long>>(schema);

            using (stream)
            {
                serialize(new ReadOnlyObservableCollection<long>(new ObservableCollection<long>(value)), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

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
                serialize(new SortedSet<string>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void StackValues(long[] value)
        {
            var schema = new ArraySchema(new LongSchema());

            var deserialize = deserializerBuilder.BuildDelegate<Stack<long>>(schema);
            var serialize = serializerBuilder.BuildDelegate<Stack<long>>(schema);

            using (stream)
            {
                serialize(new Stack<long>(value), new Utf8JsonWriter(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
