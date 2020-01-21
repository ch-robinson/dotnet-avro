using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Xunit;

namespace Chr.Avro.Serialization.Tests
{
    public class MapSerializationTests
    {
        protected readonly IBinaryDeserializerBuilder DeserializerBuilder;

        protected readonly IBinarySerializerBuilder SerializerBuilder;

        public MapSerializationTests()
        {
            DeserializerBuilder = new BinaryDeserializerBuilder();
            SerializerBuilder = new BinarySerializerBuilder();
        }

        [Theory]
        [MemberData(nameof(DateTimeKeyData))]
        public void DictionaryValues(Dictionary<DateTime, string> value)
        {
            var schema = new MapSchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<Dictionary<DateTime, string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<Dictionary<DateTime, string>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(DateTimeKeyData))]
        public void IDictionaryValues(Dictionary<DateTime, string> value)
        {
            var schema = new MapSchema(new StringSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IDictionary<DateTime, string>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IDictionary<DateTime, string>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IEnumerableValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IEnumerable<KeyValuePair<string, double>>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IEnumerable<KeyValuePair<string, double>>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IImmutableDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IImmutableDictionary<string, double>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IImmutableDictionary<string, double>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableDictionary())));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void IReadOnlyDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<IReadOnlyDictionary<string, double>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<IReadOnlyDictionary<string, double>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value)));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void ImmutableDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableDictionary<string, double>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableDictionary<string, double>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableDictionary())));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void ImmutableSortedDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<ImmutableSortedDictionary<string, double>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<ImmutableSortedDictionary<string, double>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(value.ToImmutableSortedDictionary())));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void SortedDictionaryValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<SortedDictionary<string, double>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<SortedDictionary<string, double>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(new SortedDictionary<string, double>(value))));
        }

        [Theory]
        [MemberData(nameof(StringKeyData))]
        public void SortedListValues(Dictionary<string, double> value)
        {
            var schema = new MapSchema(new DoubleSchema());

            var deserializer = DeserializerBuilder.BuildDeserializer<SortedList<string, double>>(schema);
            var serializer = SerializerBuilder.BuildSerializer<SortedList<string, double>>(schema);
            Assert.Equal(value, deserializer.Deserialize(serializer.Serialize(new SortedList<string, double>(value))));
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
