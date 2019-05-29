using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Tests
{
    public class SchemaBuilderTests
    {
        protected readonly SchemaBuilder Builder;

        public SchemaBuilderTests()
        {
            Builder = new SchemaBuilder();
        }

        [Theory]
        [InlineData(typeof(ICollection<string>), typeof(StringSchema))]
        [InlineData(typeof(IEnumerable<double>), typeof(DoubleSchema))]
        [InlineData(typeof(IList<bool>), typeof(BooleanSchema))]
        [InlineData(typeof(List<object>), typeof(RecordSchema))]
        [InlineData(typeof(List<List<object>>), typeof(ArraySchema))]
        [InlineData(typeof(int[]), typeof(IntSchema))]
        [InlineData(typeof(int[][]), typeof(ArraySchema))]
        public void BuildsArrays(Type type, Type inner)
        {
            var schema = Builder.BuildSchema(type) as ArraySchema;

            Assert.NotNull(schema);
            Assert.IsType(inner, schema.Item);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(bool))]
        public void BuildsBooleans(Type type)
        {
            var schema = Builder.BuildSchema(type) as BooleanSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(byte[]))]
        public void BuildsByteArrays(Type type)
        {
            var schema = Builder.BuildSchema(type) as BytesSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildsClassesWithFields()
        {
            var schema = Builder.BuildSchema<VisibilityClass>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.All(schema.Fields, f => Assert.IsType<IntSchema>(f.Type));
            Assert.Collection(schema.Fields,
                f => Assert.Equal(nameof(VisibilityClass.PublicField), f.Name),
                f => Assert.Equal(nameof(VisibilityClass.PublicProperty), f.Name)
            );
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(VisibilityClass).Name, schema.Name);
            Assert.Equal(typeof(VisibilityClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildsClassesWithMultipleRecursion()
        {
            var schema = Builder.BuildSchema<CircularClassA>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Collection(schema.Fields,
                f =>
                {
                    Assert.Equal(nameof(CircularClassA.B), f.Name);

                    var b = f.Type as RecordSchema;

                    Assert.NotNull(b);
                    Assert.Collection(b.Fields,
                        a =>
                        {
                            Assert.Equal(nameof(CircularClassB.A), a.Name);
                            Assert.Equal(schema, a.Type);
                        }
                    );
                    Assert.Null(b.LogicalType);
                    Assert.Equal(typeof(CircularClassB).Name, b.Name);
                    Assert.Equal(typeof(CircularClassB).Namespace, b.Namespace);
                }
            );
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(CircularClassA).Name, schema.Name);
            Assert.Equal(typeof(CircularClassA).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildsClassesWithNoFields()
        {
            var schema = Builder.BuildSchema<EmptyClass>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Empty(schema.Fields);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(EmptyClass).Name, schema.Name);
            Assert.Equal(typeof(EmptyClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildsClassesWithNullableProperties()
        {
            var schema = Builder.BuildSchema<NullablePropertyClass>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Collection(schema.Fields,
                f => {
                    Assert.Equal(nameof(NullablePropertyClass.Created), f.Name);
                    Assert.IsType<StringSchema>(f.Type);
                },
                f => {
                    Assert.Equal(nameof(NullablePropertyClass.Deleted), f.Name);
                    Assert.IsType<UnionSchema>(f.Type);
                },
                f => {
                    Assert.Equal(nameof(NullablePropertyClass.Id), f.Name);
                    Assert.IsType<StringSchema>(f.Type);
                },
                f => {
                    Assert.Equal(nameof(NullablePropertyClass.Updated), f.Name);
                    Assert.IsType<UnionSchema>(f.Type);
                }
            );
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(NullablePropertyClass).Name, schema.Name);
            Assert.Equal(typeof(NullablePropertyClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildsClassesWithSingleRecursion()
        {
            var schema = Builder.BuildSchema<CircularClass>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Collection(schema.Fields,
                f =>
                {
                    Assert.Equal(nameof(CircularClass.Child), f.Name);
                    Assert.Equal(schema, f.Type);
                }
            );
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(CircularClass).Name, schema.Name);
            Assert.Equal(typeof(CircularClass).Namespace, schema.Namespace);
        }

        [Theory]
        [InlineData(typeof(decimal))]
        public void BuildsDecimals(Type type)
        {
            var schema = Builder.BuildSchema(type) as BytesSchema;

            Assert.NotNull(schema);

            var logicalType = schema.LogicalType as DecimalLogicalType;

            Assert.NotNull(logicalType);
            Assert.Equal(29, logicalType.Precision);
            Assert.Equal(14, logicalType.Scale);
        }

        [Theory]
        [InlineData(typeof(double))]
        public void BuildsDoubles(Type type)
        {
            var schema = Builder.BuildSchema(type) as DoubleSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(TimeSpan))]
        public void BuildsDurations(Type type)
        {
            var schema = Builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildsEnumsWithDuplicateValues()
        {
            var schema = Builder.BuildSchema<DuplicateEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(DuplicateEnum).Name, schema.Name);
            Assert.Equal(typeof(DuplicateEnum).Namespace, schema.Namespace);
            Assert.Collection(schema.Symbols,
                s => Assert.Equal(nameof(DuplicateEnum.A), s),
                s => Assert.Equal(nameof(DuplicateEnum.B), s),
                s => Assert.Equal(nameof(DuplicateEnum.C), s)
            );
        }

        [Fact]
        public void BuildsEnumsWithExplicitValues()
        {
            var schema = Builder.BuildSchema<ExplicitEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(ExplicitEnum).Name, schema.Name);
            Assert.Equal(typeof(ExplicitEnum).Namespace, schema.Namespace);
            Assert.Collection(schema.Symbols,
                s => Assert.Equal(nameof(ExplicitEnum.Third), s),
                s => Assert.Equal(nameof(ExplicitEnum.First), s),
                s => Assert.Equal(nameof(ExplicitEnum.None), s),
                s => Assert.Equal(nameof(ExplicitEnum.Second), s),
                s => Assert.Equal(nameof(ExplicitEnum.Fourth), s)
            );
        }

        [Fact]
        public void BuildsEnumsWithImplicitValues()
        {
            var schema = Builder.BuildSchema<ImplicitEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(ImplicitEnum).Name, schema.Name);
            Assert.Equal(typeof(ImplicitEnum).Namespace, schema.Namespace);
            Assert.Collection(schema.Symbols,
                s => Assert.Equal(nameof(ImplicitEnum.None), s),
                s => Assert.Equal(nameof(ImplicitEnum.First), s),
                s => Assert.Equal(nameof(ImplicitEnum.Second), s),
                s => Assert.Equal(nameof(ImplicitEnum.Third), s),
                s => Assert.Equal(nameof(ExplicitEnum.Fourth), s)
            );
        }

        [Fact]
        public void BuildsEnumsWithNoSymbols()
        {
            var schema = Builder.BuildSchema<EmptyEnum>() as EnumSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(EmptyEnum).Name, schema.Name);
            Assert.Equal(typeof(EmptyEnum).Namespace, schema.Namespace);
            Assert.Empty(schema.Symbols);
        }

        [Fact]
        public void BuildsFlagEnums()
        {
            var schema = Builder.BuildSchema<FlagEnum>() as LongSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(float))]
        public void BuildsFloats(Type type)
        {
            var schema = Builder.BuildSchema(type) as FloatSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildsInterfacesWithFields()
        {
            var schema = Builder.BuildSchema<IVisibilityInterface>() as RecordSchema;

            Assert.NotNull(schema);
            Assert.Collection(schema.Fields,
                f =>
                {
                    Assert.Equal(nameof(IVisibilityInterface.PublicProperty), f.Name);
                    Assert.IsType<IntSchema>(f.Type);
                }
            );
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(IVisibilityInterface).Name, schema.Name);
            Assert.Equal(typeof(IVisibilityInterface).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildsInterfacesWithNoFields()
        {
            var schema = Builder.BuildSchema<IEmptyInterface>() as RecordSchema;

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
        public void BuildsInts(Type type)
        {
            var schema = Builder.BuildSchema(type) as IntSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(long))]
        [InlineData(typeof(ulong))]
        public void BuildsLongs(Type type)
        {
            var schema = Builder.BuildSchema(type) as LongSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(IDictionary<string, int>), typeof(IntSchema))]
        [InlineData(typeof(IDictionary<int, string>), typeof(StringSchema))]
        [InlineData(typeof(IEnumerable<KeyValuePair<string, string>>), typeof(StringSchema))]
        public void BuildsMaps(Type type, Type inner)
        {
            var schema = Builder.BuildSchema(type) as MapSchema;

            Assert.NotNull(schema);
            Assert.IsType(inner, schema.Value);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(string))]
        public void BuildsStrings(Type type)
        {
            var schema = Builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildsStructsWithNoFields()
        {
            var schema = Builder.BuildSchema<EmptyStruct>() as RecordSchema;

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
        [InlineData(typeof(FlagEnum?), typeof(LongSchema))]
        [InlineData(typeof(ImplicitEnum?), typeof(EnumSchema))]
        [InlineData(typeof(TimeSpan?), typeof(StringSchema))]
        public void BuildsNullables(Type type, Type inner)
        {
            var schema = Builder.BuildSchema(type) as UnionSchema;

            Assert.NotNull(schema);
            Assert.Collection(schema.Schemas,
                s => Assert.IsType<NullSchema>(s),
                s => Assert.IsType(inner, s)
            );
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void BuildsTimestamps(Type type)
        {
            var schema = Builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(Uri))]
        public void BuildsUris(Type type)
        {
            var schema = Builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(Guid))]
        public void BuildsUuids(Type type)
        {
            var schema = Builder.BuildSchema(type) as StringSchema;

            Assert.NotNull(schema);

            var logicalType = schema.LogicalType as UuidLogicalType;

            Assert.NotNull(logicalType);
        }

        [Theory]
        [InlineData(typeof(IntPtr))]
        [InlineData(typeof(int[,]))]
        public void ThrowsWhenNoCaseMatches(Type type)
        {
            Assert.Throws<UnsupportedTypeException>(() => Builder.BuildSchema(type));
        }
    }
}
