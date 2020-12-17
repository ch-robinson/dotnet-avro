using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Representation.Tests
{
    public class JsonRepresentationTests
    {
        protected readonly JsonSchemaReader Reader;

        protected readonly JsonSchemaWriter Writer;

        public JsonRepresentationTests()
        {
            Reader = new JsonSchemaReader();
            Writer = new JsonSchemaWriter();
        }

        [Theory]
        [MemberData(nameof(PrimitiveSchemaRepresentations))]
        public void AsymmetricRepresentations(string @out, string @in)
        {
            Assert.Equal(@out, Writer.Write(Reader.Read(@out)));
            Assert.Equal(@out, Writer.Write(Reader.Read(@in)));
        }

        [Theory]
        [MemberData(nameof(ArraySchemaRepresentations))]
        [MemberData(nameof(DateLogicalTypeRepresentations))]
        [MemberData(nameof(DecimalLogicalTypeRepresentations))]
        [MemberData(nameof(DurationLogicalTypeRepresentations))]
        [MemberData(nameof(EnumSchemaRepresentations))]
        [MemberData(nameof(FixedSchemaRepresentations))]
        [MemberData(nameof(MapSchemaRepresentations))]
        [MemberData(nameof(RecordSchemaRepresentations))]
        [MemberData(nameof(TimeLogicalTypeRepresentations))]
        [MemberData(nameof(TimestampLogicalTypeRepresentations))]
        [MemberData(nameof(UnionSchemaRepresentations))]
        [MemberData(nameof(UuidLogicalTypeRepresentations))]
        public void SymmetricRepresentations(string schema)
        {
            Assert.Equal(schema, Writer.Write(Reader.Read(schema)));
        }

        public static IEnumerable<object[]> ArraySchemaRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"array\",\"items\":\"null\"}" },
            new object[] { "{\"type\":\"array\",\"items\":\"string\"}" },
            new object[] { "{\"type\":\"array\",\"items\":[\"null\",\"string\"]}" },
        };

        public static IEnumerable<object[]> DateLogicalTypeRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"int\",\"logicalType\":\"date\"}" },
        };

        public static IEnumerable<object[]> DecimalLogicalTypeRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":6}" },
            new object[] { "{\"name\":\"temperatures.Celcius\",\"type\":\"fixed\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":3,\"size\":4}" },
        };

        public static IEnumerable<object[]> DurationLogicalTypeRepresentations => new List<object[]>
        {
            new object[] { "{\"name\":\"duration\",\"type\":\"fixed\",\"logicalType\":\"duration\",\"size\":12}" },
        };

        public static IEnumerable<object[]> EnumSchemaRepresentations => new List<object[]>
        {
            new object[] { "{\"name\":\"Empty\",\"type\":\"enum\",\"symbols\":[]}" },
            new object[] { "{\"name\":\"cards.Suit\",\"type\":\"enum\",\"symbols\":[\"CLUBS\",\"DIAMONDS\",\"HEARTS\",\"SPADES\"]}" },
        };

        public static IEnumerable<object[]> FixedSchemaRepresentations => new List<object[]>
        {
            new object[] { "{\"name\":\"Empty\",\"type\":\"fixed\",\"size\":0}" },
            new object[] { "{\"name\":\"Empty\",\"aliases\":[\"Empty\"],\"type\":\"fixed\",\"size\":0}" },
            new object[] { "{\"name\":\"sizes.Kibibyte\",\"type\":\"fixed\",\"size\":0}" },
        };

        public static IEnumerable<object[]> MapSchemaRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"map\",\"values\":\"null\"}" },
            new object[] { "{\"type\":\"map\",\"values\":\"double\"}" },
            new object[] { "{\"type\":\"map\",\"values\":[\"null\",\"double\",\"int\"]}" },
        };

        public static IEnumerable<object[]> PrimitiveSchemaRepresentations => new List<object[]>
        {
            new object[] { "\"boolean\"", "{\"type\":\"boolean\"}" },
            new object[] { "\"bytes\"", "{\"type\":\"bytes\"}" },
            new object[] { "\"double\"", "{\"type\":\"double\"}" },
            new object[] { "\"float\"", "{\"type\":\"float\"}" },
            new object[] { "\"int\"", "{\"type\":\"int\"}" },
            new object[] { "\"long\"", "{\"type\":\"long\"}" },
            new object[] { "\"null\"",  "{\"type\":\"null\"}" },
            new object[] { "\"string\"", "{\"type\":\"string\"}" },
        };

        public static IEnumerable<object[]> RecordSchemaRepresentations => new List<object[]>
        {
            new object[] { "{\"name\":\"Empty\",\"type\":\"record\",\"fields\":[]}" },
            new object[] { "{\"name\":\"Empty\",\"aliases\":[\"Empty\"],\"type\":\"record\",\"fields\":[]}" },
            new object[] { "{\"name\":\"cards.Card\",\"type\":\"record\",\"fields\":[{\"name\":\"suit\",\"type\":{\"name\":\"cards.Suit\",\"type\":\"enum\",\"symbols\":[\"CLUBS\",\"DIAMONDS\",\"HEARTS\",\"SPADES\"]}},{\"name\":\"number\",\"type\":\"int\"}]}" },
            new object[] { "{\"name\":\"lists.Node\",\"type\":\"record\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"type\":\"lists.Node\"}]}" },
        };

        public static IEnumerable<object[]> TimeLogicalTypeRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"long\",\"logicalType\":\"time-micros\"}" },
            new object[] { "{\"type\":\"int\",\"logicalType\":\"time-millis\"}" },
        };

        public static IEnumerable<object[]> TimestampLogicalTypeRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}" },
            new object[] { "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}" },
        };

        public static IEnumerable<object[]> UnionSchemaRepresentations => new List<object[]>
        {
            new object[] { "[]" },
            new object[] { "[\"string\"]" },
            new object[] { "[\"null\",\"int\"]" },
            new object[] { "[\"int\",\"null\"]" },
        };

        public static IEnumerable<object[]> UuidLogicalTypeRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"string\",\"logicalType\":\"uuid\"}" },
        };
    }
}
