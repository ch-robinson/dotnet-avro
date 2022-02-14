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

    public class MapSerializationTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public MapSerializationTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
            stream = new MemoryStream();
        }

        public static IEnumerable<object[]> DateTimeKeyData => new List<object[]>
        {
            new object[]
            {
                new Dictionary<DateTime, string>
                {
                    { new DateTime(1924, 1, 6), "Earl Scruggs" },
                    { new DateTime(1945, 8, 14), "Steve Martin" },
                    { new DateTime(1958, 7, 10), "Béla Fleck" },
                    { new DateTime(1981, 2, 27), "Noam Pikelny" },
                },
            },
        };

        public static IEnumerable<object[]> StringKeyData => new List<object[]>
        {
            new object[]
            {
                new Dictionary<string, double>
                {
                    { "e", 2.71828182845905 },
                    { "π", 3.14159265358979 },
                    { "τ", 6.28318530717958 },
                },
            },
        };

        [Theory]
        [MemberData(nameof(DateTimeKeyData))]
        public void DictionaryValues(Dictionary<DateTime, string> value)
        {
            var schema = new MapSchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<Dictionary<DateTime, string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<Dictionary<DateTime, string>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void DynamicDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<dynamic>(schema);
            var serialize = serializerBuilder.BuildDelegate<dynamic>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value.ToDictionary(p => p.Key, p => (object)p.Value), deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimeKeyData))]
        public void IDictionaryValues(Dictionary<DateTime, string> value)
        {
            var schema = new MapSchema(new StringSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IDictionary<DateTime, string>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IDictionary<DateTime, string>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IEnumerableValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IEnumerable<KeyValuePair<string, double>>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IEnumerable<KeyValuePair<string, double>>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IImmutableDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IImmutableDictionary<string, double>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IImmutableDictionary<string, double>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableDictionary(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IReadOnlyDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<IReadOnlyDictionary<string, double>>(schema);
            var serialize = serializerBuilder.BuildDelegate<IReadOnlyDictionary<string, double>>(schema);

            using (stream)
            {
                serialize(value, new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void ImmutableDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableDictionary<string, double>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableDictionary<string, double>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableDictionary(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void ImmutableSortedDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ImmutableSortedDictionary<string, double>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ImmutableSortedDictionary<string, double>>(schema);

            using (stream)
            {
                serialize(value.ToImmutableSortedDictionary(), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void ReadOnlyDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<ReadOnlyDictionary<string, double>>(schema);
            var serialize = serializerBuilder.BuildDelegate<ReadOnlyDictionary<string, double>>(schema);

            using (stream)
            {
                serialize(new ReadOnlyDictionary<string, double>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void SortedDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<SortedDictionary<string, double>>(schema);
            var serialize = serializerBuilder.BuildDelegate<SortedDictionary<string, double>>(schema);

            using (stream)
            {
                serialize(new SortedDictionary<string, double>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void SortedListValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = deserializerBuilder.BuildDelegate<SortedList<string, double>>(schema);
            var serialize = serializerBuilder.BuildDelegate<SortedList<string, double>>(schema);

            using (stream)
            {
                serialize(new SortedList<string, double>(value), new BinaryWriter(stream));
            }

            var reader = new BinaryReader(stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }
    }
}
