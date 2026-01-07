namespace Chr.Avro.Serialization.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Xunit;

    public class JsonSkipFieldDeserializerBuilderCaseTests
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private readonly IJsonSerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public JsonSkipFieldDeserializerBuilderCaseTests()
        {
            deserializerBuilder = new JsonDeserializerBuilder();
            serializerBuilder = new JsonSerializerBuilder();
            stream = new MemoryStream();
        }

        [Fact]
        public void Can_skip_int_field()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new IntSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, int>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "hello", SkippedField = 12345, KeptField2 = "world" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("hello", result.KeptField);
            Assert.Equal("world", result.KeptField2);
        }

        [Fact]
        public void Can_skip_string_field()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new StringSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, string>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "kept", SkippedField = "this_should_be_skipped", KeptField2 = "also_kept" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("kept", result.KeptField);
            Assert.Equal("also_kept", result.KeptField2);
        }

        [Fact]
        public void Can_skip_boolean_field()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new BooleanSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, bool>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "flag", SkippedField = true, KeptField2 = "value" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("flag", result.KeptField);
            Assert.Equal("value", result.KeptField2);
        }

        [Fact]
        public void Can_skip_double_field()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new DoubleSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, double>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "e", SkippedField = 2.71828, KeptField2 = "euler" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("e", result.KeptField);
            Assert.Equal("euler", result.KeptField2);
        }

        [Fact]
        public void Can_skip_array_field()
        {
            var arraySchema = new ArraySchema(new IntSchema());
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", arraySchema),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, int[]>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "array", SkippedField = new[] { 1, 2, 3, 4, 5 }, KeptField2 = "sequence" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("array", result.KeptField);
            Assert.Equal("sequence", result.KeptField2);
        }

        [Fact]
        public void Can_skip_map_field()
        {
            var mapSchema = new MapSchema(new IntSchema());
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", mapSchema),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, Dictionary<string, int>>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            var data = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };

            using (stream)
            {
                serialize(
                    new() { KeptField = "map", SkippedField = data, KeptField2 = "dictionary" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("map", result.KeptField);
            Assert.Equal("dictionary", result.KeptField2);
        }

        [Fact]
        public void Can_skip_nested_record_field()
        {
            var nestedSchema = new RecordSchema("NestedRecord", new RecordField[]
            {
                new("nested_field", new IntSchema()),
            });

            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", nestedSchema),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, NestedRecord>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new()
                    {
                        KeptField = "outer",
                        SkippedField = new NestedRecord { NestedField = 999 },
                        KeptField2 = "nested",
                    },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("outer", result.KeptField);
            Assert.Equal("nested", result.KeptField2);
        }

        [Fact]
        public void Can_skip_multiple_fields()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new IntSchema()),
                new("KeptField2", new StringSchema()),
                new("SkippedField2", new LongSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<MultiFieldRecord>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new()
                    {
                        KeptField = "first",
                        SkippedField = 100,
                        KeptField2 = "second",
                        SkippedField2 = 200,
                    },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("first", result.KeptField);
            Assert.Equal("second", result.KeptField2);
        }

        [Fact]
        public void Can_skip_enum_field_with_symbolic_behavior()
        {
            var enumSchema = new EnumSchema("CustomEnum", new[] { "A", "B", "C" });
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", enumSchema),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, CustomEnum>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "status", SkippedField = CustomEnum.C, KeptField2 = "enum_type" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("status", result.KeptField);
            Assert.Equal("enum_type", result.KeptField2);
        }

        [Fact]
        public void Can_skip_enum_field_with_integral_behavior()
        {
            // Integral behavior: enum is serialized as int
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new IntSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, CustomEnum>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "status", SkippedField = CustomEnum.C, KeptField2 = "value" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("status", result.KeptField);
            Assert.Equal("value", result.KeptField2);
        }

        [Fact]
        public void Can_skip_enum_field_with_nominal_behavior()
        {
            // Nominal behavior: enum is serialized as string
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new StringSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, CustomEnum>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "status", SkippedField = CustomEnum.C, KeptField2 = "value" },
                    new(stream));
            }

            var reader = new Utf8JsonReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("status", result.KeptField);
            Assert.Equal("value", result.KeptField2);
        }

        public class Record<TKept>
        {
            public TKept KeptField { get; set; }

            public TKept KeptField2 { get; set; }
        }

        public class Record<TKept, TSkipped>
        {
            public TKept KeptField { get; set; }

            public TSkipped SkippedField { get; set; }

            public TKept KeptField2 { get; set; }
        }

        public class NestedRecord
        {
            public int NestedField { get; set; }
        }

        public class MultiFieldRecord
        {
            public string KeptField { get; set; }

            public int SkippedField { get; set; }

            public string KeptField2 { get; set; }

            public long SkippedField2 { get; set; }
        }

        private enum CustomEnum
        {
            A,
            B,
            C,
        }
    }
}
