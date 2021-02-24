namespace Chr.Avro.Tests
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Xunit;

    public class SchemaBuilderShould
    {
        private readonly SchemaBuilder builder;

        public SchemaBuilderShould()
        {
            builder = new SchemaBuilder();
        }

        [Theory]
        [InlineData(typeof(ICollection<string>), typeof(StringSchema))]
        [InlineData(typeof(IEnumerable<double>), typeof(DoubleSchema))]
        [InlineData(typeof(IList<bool>), typeof(BooleanSchema))]
        [InlineData(typeof(List<object>), typeof(RecordSchema))]
        [InlineData(typeof(List<List<object>>), typeof(ArraySchema))]
        [InlineData(typeof(int[]), typeof(IntSchema))]
        [InlineData(typeof(int[][]), typeof(ArraySchema))]
        public void BuildArrays(Type type, Type inner)
        {
            var schema = builder.BuildSchema(type) as ArraySchema;

            Assert.NotNull(schema);
            Assert.IsType(inner, schema.Item);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(bool))]
        public void BuildBooleans(Type type)
        {
            var schema = builder.BuildSchema(type) as BooleanSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(byte[]))]
        public void BuildByteArrays(Type type)
        {
            var schema = builder.BuildSchema(type) as BytesSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildClassesWithFields()
        {
            var schema = builder.BuildSchema<VisibilityClass>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.All(schema.Fields, f => Assert.IsType<IntSchema>(f.Type));
            Assert.Collection(
                schema.Fields,
                f => Assert.Equal(nameof(VisibilityClass.PublicField), f.Name),
                f => Assert.Equal(nameof(VisibilityClass.PublicProperty), f.Name));
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(VisibilityClass).Name, schema.Name);
            Assert.Equal(typeof(VisibilityClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithMultipleRecursion()
        {
            var schema = builder.BuildSchema<CircularClassA>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Collection(
                schema.Fields,
                f =>
                {
                    Assert.Equal(nameof(CircularClassA.B), f.Name);

                    var b = f.Type as RecordSchema;

                    Assert.NotNull(b);
                    Assert.Collection(
                        b.Fields,
                        a =>
                        {
                            Assert.Equal(nameof(CircularClassB.A), a.Name);
                            Assert.Equal(schema, a.Type);
                        });
                    Assert.Null(b.LogicalType);
                    Assert.Equal(typeof(CircularClassB).Name, b.Name);
                    Assert.Equal(typeof(CircularClassB).Namespace, b.Namespace);
                });
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(CircularClassA).Name, schema.Name);
            Assert.Equal(typeof(CircularClassA).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithNoFields()
        {
            var schema = builder.BuildSchema<EmptyClass>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Empty(schema.Fields);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(EmptyClass).Name, schema.Name);
            Assert.Equal(typeof(EmptyClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithNullableProperties()
        {
            var context = new SchemaBuilderContext();
            var schema = builder.BuildSchema<NullablePropertyClass>(context) as RecordSchema;

            Assert.Collection(
                context.Schemas.Keys,
                t => t.Equals(typeof(DateTime)),
                t => t.Equals(typeof(DateTime?)),
                t => t.Equals(typeof(Guid)),
                t => t.Equals(typeof(NullablePropertyClass)));

            Assert.NotNull(schema);
            Assert.Collection(
                schema.Fields,
                f =>
                {
                    Assert.Equal(nameof(NullablePropertyClass.Created), f.Name);
                    Assert.IsType<StringSchema>(f.Type);
                },
                f =>
                {
                    Assert.Equal(nameof(NullablePropertyClass.Deleted), f.Name);
                    Assert.IsType<UnionSchema>(f.Type);
                },
                f =>
                {
                    Assert.Equal(nameof(NullablePropertyClass.Id), f.Name);
                    Assert.IsType<StringSchema>(f.Type);
                },
                f =>
                {
                    Assert.Equal(nameof(NullablePropertyClass.Updated), f.Name);
                    Assert.IsType<UnionSchema>(f.Type);
                });
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(NullablePropertyClass).Name, schema.Name);
            Assert.Equal(typeof(NullablePropertyClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithSingleRecursion()
        {
            var schema = builder.BuildSchema<CircularClass>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Collection(
                schema.Fields,
                f =>
                {
                    Assert.Equal(nameof(CircularClass.Child), f.Name);
                    Assert.Equal(schema, f.Type);
                });
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(CircularClass).Name, schema.Name);
            Assert.Equal(typeof(CircularClass).Namespace, schema.Namespace);
        }

        [Theory]
        [InlineData(typeof(decimal))]
        public void BuildDecimals(Type type)
        {
            var schema = builder.BuildSchema(type) as BytesSchema;

            Assert.NotNull(schema);

            var logicalType = schema.LogicalType as DecimalLogicalType;

            Assert.NotNull(logicalType);
            Assert.Equal(29, logicalType.Precision);
            Assert.Equal(14, logicalType.Scale);
        }

        [Theory]
        [InlineData(typeof(double))]
        public void BuildDoubles(Type type)
        {
            var schema = builder.BuildSchema(type) as DoubleSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(TimeSpan))]
        public void BuildDurations(Type type)
        {
            var schema = builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildEnumsWithDuplicateValues()
        {
            var schema = builder.BuildSchema<DuplicateEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(DuplicateEnum).Name, schema.Name);
            Assert.Equal(typeof(DuplicateEnum).Namespace, schema.Namespace);
            Assert.Collection(
                schema.Symbols,
                s => Assert.Equal(nameof(DuplicateEnum.A), s),
                s => Assert.Equal(nameof(DuplicateEnum.B), s),
                s => Assert.Equal(nameof(DuplicateEnum.C), s));
        }

        [Fact]
        public void BuildEnumsWithExplicitValues()
        {
            var schema = builder.BuildSchema<ExplicitEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(ExplicitEnum).Name, schema.Name);
            Assert.Equal(typeof(ExplicitEnum).Namespace, schema.Namespace);
            Assert.Collection(
                schema.Symbols,
                s => Assert.Equal(nameof(ExplicitEnum.Third), s),
                s => Assert.Equal(nameof(ExplicitEnum.First), s),
                s => Assert.Equal(nameof(ExplicitEnum.None), s),
                s => Assert.Equal(nameof(ExplicitEnum.Second), s),
                s => Assert.Equal(nameof(ExplicitEnum.Fourth), s));
        }

        [Fact]
        public void BuildEnumsWithImplicitValues()
        {
            var schema = builder.BuildSchema<ImplicitEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(ImplicitEnum).Name, schema.Name);
            Assert.Equal(typeof(ImplicitEnum).Namespace, schema.Namespace);
            Assert.Collection(
                schema.Symbols,
                s => Assert.Equal(nameof(ImplicitEnum.None), s),
                s => Assert.Equal(nameof(ImplicitEnum.First), s),
                s => Assert.Equal(nameof(ImplicitEnum.Second), s),
                s => Assert.Equal(nameof(ImplicitEnum.Third), s),
                s => Assert.Equal(nameof(ExplicitEnum.Fourth), s));
        }

        [Fact]
        public void BuildEnumsWithNoSymbols()
        {
            var schema = builder.BuildSchema<EmptyEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(EmptyEnum).Name, schema.Name);
            Assert.Equal(typeof(EmptyEnum).Namespace, schema.Namespace);
            Assert.Empty(schema.Symbols);
        }

        [Theory]
        [InlineData(typeof(FlagEnum), typeof(IntSchema))]
        [InlineData(typeof(LongFlagEnum), typeof(LongSchema))]
        [InlineData(typeof(UIntFlagEnum), typeof(IntSchema))]
        public void BuildFlagEnums(Type enumType, Type schemaType)
        {
            var schema = builder.BuildSchema(enumType);

            Assert.IsType(schemaType, schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(float))]
        public void BuildFloats(Type type)
        {
            var schema = builder.BuildSchema(type) as FloatSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildInterfacesWithFields()
        {
            var schema = builder.BuildSchema<IVisibilityInterface>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Collection(
                schema.Fields,
                f =>
                {
                    Assert.Equal(nameof(IVisibilityInterface.PublicProperty), f.Name);
                    Assert.IsType<IntSchema>(f.Type);
                });
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(IVisibilityInterface).Name, schema.Name);
            Assert.Equal(typeof(IVisibilityInterface).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildInterfacesWithNoFields()
        {
            var schema = builder.BuildSchema<IEmptyInterface>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Empty(schema.Fields);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(IEmptyInterface).Name, schema.Name);
            Assert.Equal(typeof(IEmptyInterface).Namespace, schema.Namespace);
        }

        [Theory]
        [InlineData(typeof(byte))]
        [InlineData(typeof(char))]
        [InlineData(typeof(int))]
        [InlineData(typeof(sbyte))]
        [InlineData(typeof(short))]
        [InlineData(typeof(uint))]
        [InlineData(typeof(ushort))]
        public void BuildInts(Type type)
        {
            var schema = builder.BuildSchema(type) as IntSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(long))]
        [InlineData(typeof(ulong))]
        public void BuildLongs(Type type)
        {
            var schema = builder.BuildSchema(type) as LongSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(IDictionary<string, int>), typeof(IntSchema))]
        [InlineData(typeof(IDictionary<int, string>), typeof(StringSchema))]
        [InlineData(typeof(IEnumerable<KeyValuePair<string, string>>), typeof(StringSchema))]
        public void BuildMaps(Type type, Type inner)
        {
            var schema = builder.BuildSchema(type) as MapSchema;

            Assert.NotNull(schema);
            Assert.IsType(inner, schema.Value);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(string))]
        public void BuildStrings(Type type)
        {
            var schema = builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildStructsWithNoFields()
        {
            var schema = builder.BuildSchema<EmptyStruct>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Empty(schema.Fields);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(EmptyStruct).Name, schema.Name);
            Assert.Equal(typeof(EmptyStruct).Namespace, schema.Namespace);
        }

        [Theory]
        [InlineData(typeof(bool?), typeof(BooleanSchema))]
        [InlineData(typeof(byte?), typeof(IntSchema))]
        [InlineData(typeof(char?), typeof(IntSchema))]
        [InlineData(typeof(decimal?), typeof(BytesSchema))]
        [InlineData(typeof(double?), typeof(DoubleSchema))]
        [InlineData(typeof(float?), typeof(FloatSchema))]
        [InlineData(typeof(int?), typeof(IntSchema))]
        [InlineData(typeof(long?), typeof(LongSchema))]
        [InlineData(typeof(sbyte?), typeof(IntSchema))]
        [InlineData(typeof(short?), typeof(IntSchema))]
        [InlineData(typeof(uint?), typeof(IntSchema))]
        [InlineData(typeof(ulong?), typeof(LongSchema))]
        [InlineData(typeof(ushort?), typeof(IntSchema))]
        [InlineData(typeof(DuplicateEnum?), typeof(EnumSchema))]
        [InlineData(typeof(EmptyEnum?), typeof(EnumSchema))]
        [InlineData(typeof(EmptyStruct?), typeof(RecordSchema))]
        [InlineData(typeof(ExplicitEnum?), typeof(EnumSchema))]
        [InlineData(typeof(FlagEnum?), typeof(IntSchema))]
        [InlineData(typeof(ImplicitEnum?), typeof(EnumSchema))]
        [InlineData(typeof(LongFlagEnum?), typeof(LongSchema))]
        [InlineData(typeof(TimeSpan?), typeof(StringSchema))]
        [InlineData(typeof(UIntFlagEnum?), typeof(IntSchema))]
        public void BuildNullables(Type type, Type inner)
        {
            var schema = builder.BuildSchema(type) as UnionSchema;

            Assert.NotNull(schema);
            Assert.Collection(
                schema.Schemas,
                s => Assert.IsType<NullSchema>(s),
                s => Assert.IsType(inner, s));
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void BuildTimestampsAsIso8601Strings(Type type)
        {
            var builder = new SchemaBuilder(SchemaBuilder.CreateDefaultCaseBuilders(TemporalBehavior.Iso8601));
            var schema = builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void BuildTimestampsAsMicrosecondsFromEpoch(Type type)
        {
            var builder = new SchemaBuilder(SchemaBuilder.CreateDefaultCaseBuilders(TemporalBehavior.EpochMicroseconds));
            var schema = builder.BuildSchema(type) as LongSchema;

            Assert.NotNull(schema);
            Assert.IsType<MicrosecondTimestampLogicalType>(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void BuildTimestampsAsMillisecondsFromEpoch(Type type)
        {
            var builder = new SchemaBuilder(SchemaBuilder.CreateDefaultCaseBuilders(TemporalBehavior.EpochMilliseconds));
            var schema = builder.BuildSchema(type) as LongSchema;

            Assert.NotNull(schema);
            Assert.IsType<MillisecondTimestampLogicalType>(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(Uri))]
        public void BuildUris(Type type)
        {
            var schema = builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(Guid))]
        public void BuildUuids(Type type)
        {
            var schema = builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);

            var logicalType = schema.LogicalType as UuidLogicalType;

            Assert.NotNull(logicalType);
        }

        [Theory]
        [InlineData(typeof(IntPtr))]
        [InlineData(typeof(int[,]))]
        public void ThrowWhenNoCaseMatches(Type type)
        {
            Assert.Throws<UnsupportedTypeException>(() => builder.BuildSchema(type));
        }
    }
}
