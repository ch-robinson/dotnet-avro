using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class MapSerializationTests
    {
        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly MemoryStream _stream;

        public MapSerializationTests()
        {
            _deserializerBuilder = new BinaryDeserializerBuilder();
            _serializerBuilder = new BinarySerializerBuilder();
            _stream = new MemoryStream();
        }

        [Theory]
        [MemberData(nameof(DateTimeKeyData))]
        public void DictionaryValues(Dictionary<DateTime, string> value)
        {
            var schema = new MapSchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<Dictionary<DateTime, string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<Dictionary<DateTime, string>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(DateTimeKeyData))]
        public void IDictionaryValues(Dictionary<DateTime, string> value)
        {
            var schema = new MapSchema(new StringSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IDictionary<DateTime, string>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IDictionary<DateTime, string>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IEnumerableValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IEnumerable<KeyValuePair<string, double>>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IEnumerable<KeyValuePair<string, double>>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IImmutableDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IImmutableDictionary<string, double>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IImmutableDictionary<string, double>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableDictionary(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IReadOnlyDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<IReadOnlyDictionary<string, double>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<IReadOnlyDictionary<string, double>>(schema);

            using (_stream)
            {
                serialize(value, new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void ImmutableDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableDictionary<string, double>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableDictionary<string, double>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableDictionary(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void ImmutableSortedDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<ImmutableSortedDictionary<string, double>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<ImmutableSortedDictionary<string, double>>(schema);

            using (_stream)
            {
                serialize(value.ToImmutableSortedDictionary(), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void SortedDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<SortedDictionary<string, double>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<SortedDictionary<string, double>>(schema);

            using (_stream)
            {
                serialize(new SortedDictionary<string, double>(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void SortedListValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserialize = _deserializerBuilder.BuildDelegate<SortedList<string, double>>(schema);
            var serialize = _serializerBuilder.BuildDelegate<SortedList<string, double>>(schema);

            using (_stream)
            {
                serialize(new SortedList<string, double>(value), new BinaryWriter(_stream));
            }

            var reader = new BinaryReader(_stream.ToArray());

            Assert.Equal(value, deserialize(ref reader));
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
                }
            }
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
                }
            }
        };
    }
}
