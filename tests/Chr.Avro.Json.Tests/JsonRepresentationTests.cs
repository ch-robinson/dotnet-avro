namespace Chr.Avro.Representation.Tests
{
    using System.Collections.Generic;
    using Chr.Avro.Abstract;
    using Xunit;

    public class JsonRepresentationTests
    {
        private readonly JsonSchemaReader reader;

        private readonly JsonSchemaWriter writer;

        public JsonRepresentationTests()
        {
            reader = new JsonSchemaReader();
            writer = new JsonSchemaWriter();
        }

        public static IEnumerable<object[]> AliasedNamedSchemaRepresentations => new List<object[]>
        {
            new object[]
            {
                "{\"name\":\"lists.Node\",\"aliases\":[\"N\"],\"type\":\"record\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"type\":[\"null\",\"lists.Node\"]}]}",
                "{\"name\":\"lists.Node\",\"aliases\":[\"N\"],\"type\":\"record\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"type\":[\"null\",\"N\"]}]}",
            },
        };

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
            new object[] { "{\"name\":\"measurements.TemperatureScale\",\"default\":\"CELSIUS\",\"type\":\"enum\",\"symbols\":[\"FAHRENHEIT\",\"CELSIUS\"]}" },
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

        public static IEnumerable<object[]> OptionalFieldRepresentations => new List<object[]>
        {
            new object[] { "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":29,\"scale\":0}", "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":29}" },
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
            new object[] { "{\"name\":\"cards.Card\",\"type\":\"record\",\"fields\":[{\"name\":\"suit\",\"type\":{\"name\":\"cards.Suit\",\"type\":\"enum\",\"symbols\":[\"CLUBS\",\"DIAMONDS\",\"HEARTS\",\"SPADES\"]}},{\"name\":\"number\",\"type\":\"int\"}]}" },
            new object[] { "{\"name\":\"lists.Node\",\"type\":\"record\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"type\":[\"null\",\"lists.Node\"]}]}" },
            new object[] { "{\"name\":\"lists.Node\",\"type\":\"record\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"default\":null,\"type\":[\"null\",\"lists.Node\"]}]}" },
            new object[] { "{\"name\":\"measurements.Temperature\",\"type\":\"record\",\"fields\":[{\"name\":\"scale\",\"default\":\"CELSIUS\",\"type\":{\"name\":\"measurements.TemperatureScale\",\"type\":\"enum\",\"symbols\":[\"FAHRENHEIT\",\"CELSIUS\"]}}]}" },
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

        [Theory]
        [MemberData(nameof(AliasedNamedSchemaRepresentations))]
        [MemberData(nameof(OptionalFieldRepresentations))]
        [MemberData(nameof(PrimitiveSchemaRepresentations))]
        public void AsymmetricRepresentations(string @out, string @in)
        {
            Assert.Equal(@out, writer.Write(reader.Read(@out)));
            Assert.Equal(@out, writer.Write(reader.Read(@in)));
        }

        [Fact]
        public void DefaultValueRepresentations()
        {
            var union = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            var schema = new RecordSchema("DefaultValueTest", new[]
            {
                new RecordField("maybeString", union)
                {
                    Default = new ObjectDefaultValue<string>(null, union),
                },
            });

            Assert.Equal("{\"name\":\"DefaultValueTest\",\"type\":\"record\",\"fields\":[{\"name\":\"maybeString\",\"default\":null,\"type\":[\"null\",\"string\"]}]}", writer.Write(schema));
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
        public void SymmetricRepresentations(string schema, string expectedOverride = null)
        {
            Assert.Equal(expectedOverride ?? schema, writer.Write(reader.Read(schema)));
        }
    }
}
