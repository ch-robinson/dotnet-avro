using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
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
