using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class ArraySerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public ArraySerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ArrayValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<int[]>(schema);
            var serialize = _serializerBuilder.BuildDelegate<int[]>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ArraySegmentValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ArraySegment<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ArraySegment<int>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void CollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<Collection<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Collection<int>>(schema);

            using (_stream)
            {
                serialize(new Collection<int>(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void HashSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<HashSet<string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<HashSet<string>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ICollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ICollection<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ICollection<int>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IEnumerableValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IEnumerable<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IEnumerable<int>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IImmutableList<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IImmutableList<int>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableList(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableQueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IImmutableQueue<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IImmutableQueue<int>>(schema);

            using (_stream)
            {
                serialize(ImmutableQueue.CreateRange(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void IImmutableSetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IImmutableSet<string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IImmutableSet<string>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableHashSet(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IImmutableStackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IImmutableStack<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IImmutableStack<int>>(schema);

            using (_stream)
            {
                serialize(ImmutableStack.CreateRange(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IList<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IList<int>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyCollectionValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IReadOnlyCollection<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IReadOnlyCollection<int>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void IReadOnlyListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IReadOnlyList<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IReadOnlyList<int>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ISetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ISet<string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ISet<string>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableArrayValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableArray<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableArray<int>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableArray(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ImmutableHashSetValues(ISet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableHashSet<string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableHashSet<string>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableHashSet(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader).OrderBy(v => v));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableList<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableList<int>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableList(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableQueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableQueue<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableQueue<int>>(schema);

            using (_stream)
            {
                serialize(ImmutableQueue.CreateRange(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void ImmutableSortedSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableSortedSet<string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableSortedSet<string>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableSortedSet(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ImmutableStackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableStack<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableStack<int>>(schema);

            using (_stream)
            {
                serialize(ImmutableStack.CreateRange(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(JaggedArrayData))]
        public void JaggedArrayValues(string[][] value)
        {
            var schema = new ArraySchema(new ArraySchema(new StringSchema()));

            var deserialize = _deserializerBuilder.BuildDelegate<string[][]>(schema);
            var serialize = _serializerBuilder.BuildDelegate<string[][]>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void LinkedListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<LinkedList<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<LinkedList<int>>(schema);

            using (_stream)
            {
                serialize(new LinkedList<int>(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void ListValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<List<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<List<int>>(schema);

            using (_stream)
            {
                serialize(value.ToList(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void QueueValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<Queue<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Queue<int>>(schema);

            using (_stream)
            {
                serialize(new Queue<int>(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(SetData))]
        public void SortedSetValues(HashSet<string> value)
        {
            var schema = new ArraySchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<SortedSet<string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<SortedSet<string>>(schema);

            using (_stream)
            {
                serialize(new SortedSet<string>(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(ArrayData))]
        public void StackValues(int[] value)
        {
            var schema = new ArraySchema(new IntSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<Stack<int>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Stack<int>>(schema);

            using (_stream)
            {
                serialize(new Stack<int>(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
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
