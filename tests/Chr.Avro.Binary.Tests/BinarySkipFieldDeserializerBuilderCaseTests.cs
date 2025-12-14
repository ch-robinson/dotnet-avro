namespace Chr.Avro.Serialization.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using Chr.Avro.Abstract;
    using Xunit;

    using BinaryReader = Chr.Avro.Serialization.BinaryReader;
    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class BinarySkipFieldDeserializerBuilderCaseTests
    {
        private readonly IBinaryDeserializerBuilder deserializerBuilder;

        private readonly IBinarySerializerBuilder serializerBuilder;

        private readonly MemoryStream stream;

        public BinarySkipFieldDeserializerBuilderCaseTests()
        {
            deserializerBuilder = new BinaryDeserializerBuilder();
            serializerBuilder = new BinarySerializerBuilder();
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

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("hello", result.KeptField);
            Assert.Equal("world", result.KeptField2);
        }

        [Fact]
        public void Can_skip_long_field()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new LongSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, long>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "world", SkippedField = 9876543210, KeptField2 = "test" },
                    new(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("world", result.KeptField);
            Assert.Equal("test", result.KeptField2);
        }

        [Fact]
        public void Can_skip_float_field()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new FloatSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, float>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "pi", SkippedField = 3.14f, KeptField2 = "math" },
                    new(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("pi", result.KeptField);
            Assert.Equal("math", result.KeptField2);
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

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("e", result.KeptField);
            Assert.Equal("euler", result.KeptField2);
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

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("flag", result.KeptField);
            Assert.Equal("value", result.KeptField2);
        }

        [Fact]
        public void Can_skip_bytes_field()
        {
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", new BytesSchema()),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, byte[]>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "data", SkippedField = new byte[] { 0xAA, 0xBB, 0xCC }, KeptField2 = "binary" },
                    new(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("data", result.KeptField);
            Assert.Equal("binary", result.KeptField2);
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

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("kept", result.KeptField);
            Assert.Equal("also_kept", result.KeptField2);
        }

        [Fact]
        public void Can_skip_fixed_field()
        {
            var fixedSchema = new FixedSchema("FixedData", 4);
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", fixedSchema),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, byte[]>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "fixed", SkippedField = new byte[] { 0x01, 0x02, 0x03, 0x04 }, KeptField2 = "size4" },
                    new(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("fixed", result.KeptField);
            Assert.Equal("size4", result.KeptField2);
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

            var reader = new BinaryReader(stream.ToArray());
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

            var serialize = serializerBuilder.BuildDelegate<Record<string, int>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "status", SkippedField = 1, KeptField2 = "value" },
                    new(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
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

            var serialize = serializerBuilder.BuildDelegate<Record<string, string>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "status", SkippedField = "A", KeptField2 = "value" },
                    new(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("status", result.KeptField);
            Assert.Equal("value", result.KeptField2);
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

            var reader = new BinaryReader(stream.ToArray());
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

            var reader = new BinaryReader(stream.ToArray());
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

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("outer", result.KeptField);
            Assert.Equal("nested", result.KeptField2);
        }

        [Fact]
        public void Can_skip_union_field()
        {
            var unionSchema = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            var schema = new RecordSchema("TestRecord", new RecordField[]
            {
                new("KeptField", new StringSchema()),
                new("SkippedField", unionSchema),
                new("KeptField2", new StringSchema()),
            });

            var serialize = serializerBuilder.BuildDelegate<Record<string, string>>(schema);
            var deserialize = deserializerBuilder.BuildDelegate<Record<string>>(schema);

            using (stream)
            {
                serialize(
                    new() { KeptField = "union", SkippedField = "union_value", KeptField2 = "variant" },
                    new(stream));
            }

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("union", result.KeptField);
            Assert.Equal("variant", result.KeptField2);
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

            var reader = new BinaryReader(stream.ToArray());
            var result = deserialize(ref reader);

            Assert.Equal("first", result.KeptField);
            Assert.Equal("second", result.KeptField2);
        }

        private enum CustomEnum
        {
            A,
            B,
            C,
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
    }
}
