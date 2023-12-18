namespace Chr.Avro.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
            var schema = Assert.IsType<ArraySchema>(builder.BuildSchema(type));
            Assert.IsType(inner, schema.Item);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(bool))]
        public void BuildBooleans(Type type)
        {
            var schema = Assert.IsType<BooleanSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(byte[]))]
        public void BuildByteArrays(Type type)
        {
            var schema = Assert.IsType<BytesSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildClassesWithDataContractAttributes()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<DataContractAnnotatedClass>());
            Assert.Equal("annotated", schema.Name);
            Assert.Equal("chr.fixtures", schema.Namespace);
            Assert.Collection(
                schema.Fields,
                field => Assert.Equal(nameof(DataContractAnnotatedClass.AnnotatedDefaultField), field.Name),
                field => Assert.Equal(nameof(DataContractAnnotatedClass.AnnotatedDefaultProperty), field.Name),
                field => Assert.Equal(nameof(DataContractAnnotatedClass.ConflictingField), field.Name),
                field => Assert.Equal("DifferentProperty", field.Name),
                field => Assert.Equal("DifferentField", field.Name));
        }

        [Fact]
        public void BuildClassesWithDefaultValues()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<DefaultValuesClass>());
            Assert.Collection(
                schema.Fields,
                field =>
                {
                    Assert.Equal(nameof(DefaultValuesClass.DefaultIntField), field.Name);
                    Assert.Equal(1, field.Default.ToObject<int>());
                },
                field =>
                {
                    Assert.Equal(nameof(DefaultValuesClass.DefaultIntProperty), field.Name);
                    Assert.Equal(1, field.Default.ToObject<int>());
                },
                field =>
                {
                    Assert.Equal(nameof(DefaultValuesClass.DefaultObjectField), field.Name);
                    Assert.Null(field.Default.ToObject<object>());
                },
                field =>
                {
                    Assert.Equal(nameof(DefaultValuesClass.DefaultObjectProperty), field.Name);
                    Assert.Null(field.Default.ToObject<object>());
                });
        }

        [Fact]
        public void BuildClassesWithFields()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<VisibilityClass>());
            Assert.All(schema.Fields, f => Assert.IsType<IntSchema>(f.Type));
            Assert.Collection(
                schema.Fields,
                field => Assert.Equal(nameof(VisibilityClass.PublicField), field.Name),
                field => Assert.Equal(nameof(VisibilityClass.PublicProperty), field.Name));
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(VisibilityClass).Name, schema.Name);
            Assert.Equal(typeof(VisibilityClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithGenericParameters()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<GenericClass<string>>());
            Assert.Collection(
                schema.Fields,
                field =>
                {
                    Assert.Equal(nameof(GenericClass<string>.Item), field.Name);
                    Assert.IsType<StringSchema>(field.Type);
                });
            Assert.Null(schema.LogicalType);
            Assert.Equal("GenericClass_String", schema.Name);
            Assert.Equal(typeof(GenericClass<string>).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithMultipleRecursion()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<CircularClassA>());
            Assert.Collection(
                schema.Fields,
                field =>
                {
                    Assert.Equal(nameof(CircularClassA.B), field.Name);

                    var b = Assert.IsType<RecordSchema>(field.Type);
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
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<EmptyClass>());
            Assert.Empty(schema.Fields);
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(EmptyClass).Name, schema.Name);
            Assert.Equal(typeof(EmptyClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithIgnoredAndNonSerializedMemberAttributes()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<DataContractNonAnnotatedClass>());
            Assert.Collection(
                schema.Fields,
                field => Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedCustomField), field.Name),
                field => Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedCustomProperty), field.Name),
                field => Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedDefaultField), field.Name),
                field => Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedDefaultProperty), field.Name),
                field => Assert.Equal(nameof(DataContractNonAnnotatedClass.UnannotatedField), field.Name),
                field => Assert.Equal(nameof(DataContractNonAnnotatedClass.UnannotatedProperty), field.Name));
        }

        [Fact]
        public void BuildClassesWithDescriptionAttributes()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<DescriptionAnnotatedClass>());
            Assert.NotNull(schema.Documentation);
            Assert.NotNull(schema.Fields.First(f => f.Name == nameof(DescriptionAnnotatedClass.DescriptionField)).Documentation);
        }

        [Fact]
        public void BuildEnumsWithDescriptionAttributes()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<DescriptionAnnotatedEnum>());
            Assert.NotNull(schema.Documentation);
        }

        [Fact]
        public void BuildClassesWithNullableProperties()
        {
            var context = new SchemaBuilderContext();
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<NullableMemberClass>(context));

            Assert.Collection(
                context.Schemas.Keys,
                type => Assert.Equal(typeof(NullableMemberClass), type),
                type => Assert.Equal(typeof(string[]), type),
                type => Assert.Equal(typeof(string), type),
                type => Assert.Equal(typeof(Dictionary<string, object>), type),
                type => Assert.Equal(typeof(object), type),
                type => Assert.Equal(typeof(Guid), type),
                type => Assert.Equal(typeof(List<string>), type),
                type => Assert.Equal(typeof(Guid?), type),
                type => Assert.Equal(typeof(Uri), type));

            Assert.Collection(
                schema.Fields,
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ArrayOfNullableStringsProperty), field.Name);

                    var array = Assert.IsType<ArraySchema>(field.Type);
                    var union = Assert.IsType<UnionSchema>(array.Item);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            Assert.IsType<StringSchema>(child);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ArrayOfStringsProperty), field.Name);

                    var array = Assert.IsType<ArraySchema>(field.Type);
                    Assert.IsType<StringSchema>(array.Item);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.DictionaryOfNullableObjectsProperty), field.Name);

                    var map = Assert.IsType<MapSchema>(field.Type);
                    var union = Assert.IsType<UnionSchema>(map.Value);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            Assert.IsType<RecordSchema>(child);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.DictionaryOfObjectsProperty), field.Name);

                    var map = Assert.IsType<MapSchema>(field.Type);
                    Assert.IsType<RecordSchema>(map.Value);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.GuidProperty), field.Name);
                    Assert.IsType<StringSchema>(field.Type);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ListOfNullableStringsProperty), field.Name);

                    var array = Assert.IsType<ArraySchema>(field.Type);
                    var union = Assert.IsType<UnionSchema>(array.Item);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            Assert.IsType<StringSchema>(child);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ListOfStringsProperty), field.Name);

                    var array = Assert.IsType<ArraySchema>(field.Type);
                    Assert.IsType<StringSchema>(array.Item);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableArrayOfNullableStringsProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            var array = Assert.IsType<ArraySchema>(child);
                            var union = Assert.IsType<UnionSchema>(array.Item);
                            Assert.Collection(
                                union.Schemas,
                                child =>
                                {
                                    Assert.IsType<NullSchema>(child);
                                },
                                child =>
                                {
                                    Assert.IsType<StringSchema>(child);
                                });
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableArrayOfStringsProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            var array = Assert.IsType<ArraySchema>(child);
                            Assert.IsType<StringSchema>(array.Item);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableDictionaryOfNullableObjectsProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            var map = Assert.IsType<MapSchema>(child);
                            var union = Assert.IsType<UnionSchema>(map.Value);
                            Assert.Collection(
                                union.Schemas,
                                child =>
                                {
                                    Assert.IsType<NullSchema>(child);
                                },
                                child =>
                                {
                                    Assert.IsType<RecordSchema>(child);
                                });
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableDictionaryOfObjectsProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            var map = Assert.IsType<MapSchema>(child);
                            Assert.IsType<RecordSchema>(map.Value);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableGuidProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            Assert.IsType<StringSchema>(child);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableListOfNullableStringsProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            var array = Assert.IsType<ArraySchema>(child);
                            var union = Assert.IsType<UnionSchema>(array.Item);
                            Assert.Collection(
                                union.Schemas,
                                child =>
                                {
                                    Assert.IsType<NullSchema>(child);
                                },
                                child =>
                                {
                                    Assert.IsType<StringSchema>(child);
                                });
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableListOfStringsProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            var array = Assert.IsType<ArraySchema>(child);
                            Assert.IsType<StringSchema>(array.Item);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableStringProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            Assert.IsType<StringSchema>(child);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.NullableUriProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            Assert.IsType<StringSchema>(child);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ObliviousArrayOfStringsProperty), field.Name);

                    var array = Assert.IsType<ArraySchema>(field.Type);
                    Assert.IsType<StringSchema>(array.Item);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ObliviousDictionaryOfObjectsProperty), field.Name);

                    var map = Assert.IsType<MapSchema>(field.Type);
                    Assert.IsType<RecordSchema>(map.Value);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ObliviousGuidProperty), field.Name);
                    Assert.IsType<StringSchema>(field.Type);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ObliviousListOfStringsProperty), field.Name);

                    var array = Assert.IsType<ArraySchema>(field.Type);
                    Assert.IsType<StringSchema>(array.Item);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ObliviousNullableGuidProperty), field.Name);

                    var union = Assert.IsType<UnionSchema>(field.Type);
                    Assert.Collection(
                        union.Schemas,
                        child =>
                        {
                            Assert.IsType<NullSchema>(child);
                        },
                        child =>
                        {
                            Assert.IsType<StringSchema>(child);
                        });
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ObliviousStringProperty), field.Name);
                    Assert.IsType<StringSchema>(field.Type);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.ObliviousUriProperty), field.Name);
                    Assert.IsType<StringSchema>(field.Type);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.StringProperty), field.Name);
                    Assert.IsType<StringSchema>(field.Type);
                },
                field =>
                {
                    Assert.Equal(nameof(NullableMemberClass.UriProperty), field.Name);
                    Assert.IsType<StringSchema>(field.Type);
                });
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(NullableMemberClass).Name, schema.Name);
            Assert.Equal(typeof(NullableMemberClass).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildClassesWithSingleRecursion()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<CircularClass>());
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
#if NET6_0_OR_GREATER

        [Theory]
        [InlineData(typeof(DateOnly))]
        public void BuildDatesAsIso8601Strings(Type type)
        {
            var builder = new SchemaBuilder(temporalBehavior: TemporalBehavior.Iso8601);
            var schema = Assert.IsType<StringSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(DateOnly), TemporalBehavior.EpochMicroseconds)]
        [InlineData(typeof(DateOnly), TemporalBehavior.EpochMilliseconds)]
        public void BuildDatesAsDaysFromEpoch(Type type, TemporalBehavior temporalBehavior)
        {
            var builder = new SchemaBuilder(temporalBehavior: temporalBehavior);
            var schema = Assert.IsType<IntSchema>(builder.BuildSchema(type));
            Assert.IsType<DateLogicalType>(schema.LogicalType);
        }
#endif

        [Theory]
        [InlineData(typeof(decimal))]
        public void BuildDecimals(Type type)
        {
            var schema = Assert.IsType<BytesSchema>(builder.BuildSchema(type));
            var logicalType = Assert.IsType<DecimalLogicalType>(schema.LogicalType);
            Assert.Equal(29, logicalType.Precision);
            Assert.Equal(14, logicalType.Scale);
        }

        [Theory]
        [InlineData(nameof(RangeAnnotatedPropertiesClass.Currency), 8, 2)]
        [InlineData(nameof(RangeAnnotatedPropertiesClass.DoubleBounded), 29, 14)]
        [InlineData(nameof(RangeAnnotatedPropertiesClass.FractionOnly), 4, 3)]
        [InlineData(nameof(RangeAnnotatedPropertiesClass.NullableCurrency), 8, 2)]
        [InlineData(nameof(RangeAnnotatedPropertiesClass.WholeOnly), 1, 0)]
        public void BuildsDecimalFieldsWithSizesInferredFromRange(string fieldName, int precision, int scale)
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<RangeAnnotatedPropertiesClass>());
            var fieldSchema = schema.Fields.Single(field => field.Name == fieldName).Type;

            if (fieldSchema is UnionSchema unionSchema)
            {
                fieldSchema = unionSchema.Schemas.OfType<BytesSchema>().Single();
            }

            var logicalType = Assert.IsType<DecimalLogicalType>(fieldSchema.LogicalType);
            Assert.Equal(precision, logicalType.Precision);
            Assert.Equal(scale, logicalType.Scale);
        }

        [Theory]
        [InlineData(typeof(double))]
        public void BuildDoubles(Type type)
        {
            var schema = Assert.IsType<DoubleSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(TimeSpan))]
        public void BuildDurations(Type type)
        {
            var schema = Assert.IsType<StringSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildEnumsWithDataContractAttributes()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<DataContractAnnotatedEnum>());
            Assert.Equal("annotated", schema.Name);
            Assert.Equal("chr.fixtures", schema.Namespace);
            Assert.Collection(
                schema.Symbols,
                symbol => Assert.Equal(nameof(DataContractAnnotatedEnum.Conflicting), symbol),
                symbol => Assert.Equal(nameof(DataContractAnnotatedEnum.Default), symbol),
                symbol => Assert.Equal("Different", symbol));
        }

        [Fact]
        public void BuildEnumsWithDuplicateValues()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<DuplicateEnum>());
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(DuplicateEnum).Name, schema.Name);
            Assert.Equal(typeof(DuplicateEnum).Namespace, schema.Namespace);
            Assert.Collection(
                schema.Symbols,
                symbol => Assert.Equal(nameof(DuplicateEnum.A), symbol),
                symbol => Assert.Equal(nameof(DuplicateEnum.B), symbol),
                symbol => Assert.Equal(nameof(DuplicateEnum.C), symbol));
        }

        [Fact]
        public void BuildEnumsWithExplicitValues()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<ExplicitEnum>());
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(ExplicitEnum).Name, schema.Name);
            Assert.Equal(typeof(ExplicitEnum).Namespace, schema.Namespace);
            Assert.Collection(
                schema.Symbols,
                symbol => Assert.Equal(nameof(ExplicitEnum.Third), symbol),
                symbol => Assert.Equal(nameof(ExplicitEnum.First), symbol),
                symbol => Assert.Equal(nameof(ExplicitEnum.None), symbol),
                symbol => Assert.Equal(nameof(ExplicitEnum.Second), symbol),
                symbol => Assert.Equal(nameof(ExplicitEnum.Fourth), symbol));
        }

        [Fact]
        public void BuildEnumsWithImplicitValues()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<ImplicitEnum>());
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(ImplicitEnum).Name, schema.Name);
            Assert.Equal(typeof(ImplicitEnum).Namespace, schema.Namespace);
            Assert.Collection(
                schema.Symbols,
                symbol => Assert.Equal(nameof(ImplicitEnum.None), symbol),
                symbol => Assert.Equal(nameof(ImplicitEnum.First), symbol),
                symbol => Assert.Equal(nameof(ImplicitEnum.Second), symbol),
                symbol => Assert.Equal(nameof(ImplicitEnum.Third), symbol),
                symbol => Assert.Equal(nameof(ExplicitEnum.Fourth), symbol));
        }

        [Fact]
        public void BuildEnumsWithNoSymbols()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<EmptyEnum>());
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(EmptyEnum).Name, schema.Name);
            Assert.Equal(typeof(EmptyEnum).Namespace, schema.Namespace);
            Assert.Empty(schema.Symbols);
        }

        [Fact]
        public void BuildEnumsWithIgnoredAndNonSerializedSymbolAttributes()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<DataContractNonAnnotatedEnum>());
            Assert.Collection(
                schema.Symbols,
                symbol => Assert.Equal(nameof(DataContractNonAnnotatedEnum.None), symbol),
                symbol => Assert.Equal(nameof(DataContractNonAnnotatedEnum.Default), symbol),
                symbol => Assert.Equal(nameof(DataContractNonAnnotatedEnum.Custom), symbol));
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

        [Fact]
        public void BuildEnumsWithADefault()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<DefaultValueEnum>());
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(DefaultValueEnum).Name, schema.Name);
            Assert.Equal(typeof(DefaultValueEnum).Namespace, schema.Namespace);
            Assert.Equal(nameof(DefaultValueEnum.DefaultValue), schema.Default);
        }

        [Fact]
        public void BuildEnumsWithAnAliasedDefault()
        {
            var schema = Assert.IsType<EnumSchema>(builder.BuildSchema<DefaultValueDataContractEnum>());
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(DefaultValueDataContractEnum).Name, schema.Name);
            Assert.Equal(typeof(DefaultValueDataContractEnum).Namespace, schema.Namespace);
            Assert.Equal("AliasedDefaultValue", schema.Default);
        }

        [Theory]
        [InlineData(typeof(float))]
        public void BuildFloats(Type type)
        {
            var schema = Assert.IsType<FloatSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildInterfacesWithFields()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<IVisibilityInterface>());
            Assert.Collection(
                schema.Fields,
                field =>
                {
                    Assert.Equal(nameof(IVisibilityInterface.PublicProperty), field.Name);
                    Assert.IsType<IntSchema>(field.Type);
                });
            Assert.Null(schema.LogicalType);
            Assert.Equal(typeof(IVisibilityInterface).Name, schema.Name);
            Assert.Equal(typeof(IVisibilityInterface).Namespace, schema.Namespace);
        }

        [Fact]
        public void BuildInterfacesWithNoFields()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<IEmptyInterface>());
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
            var schema = Assert.IsType<IntSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(long))]
        [InlineData(typeof(ulong))]
        public void BuildLongs(Type type)
        {
            var schema = Assert.IsType<LongSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(IDictionary<string, int>), typeof(IntSchema))]
        [InlineData(typeof(IDictionary<int, string>), typeof(StringSchema))]
        [InlineData(typeof(IEnumerable<KeyValuePair<string, string>>), typeof(StringSchema))]
        public void BuildMaps(Type type, Type inner)
        {
            var schema = Assert.IsType<MapSchema>(builder.BuildSchema(type));
            Assert.IsType(inner, schema.Value);
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(string))]
        public void BuildStrings(Type type)
        {
            var schema = Assert.IsType<StringSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Fact]
        public void BuildStructsWithNoFields()
        {
            var schema = Assert.IsType<RecordSchema>(builder.BuildSchema<EmptyStruct>());
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
            var schema = Assert.IsType<UnionSchema>(builder.BuildSchema(type));
            Assert.Collection(
                schema.Schemas,
                s => Assert.IsType<NullSchema>(s),
                s => Assert.IsType(inner, s));
        }
#if NET6_0_OR_GREATER

        [Theory]
        [InlineData(typeof(TimeOnly))]
        public void BuildTimesAsIso8601Strings(Type type)
        {
            var builder = new SchemaBuilder(temporalBehavior: TemporalBehavior.Iso8601);
            var schema = Assert.IsType<StringSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(TimeOnly))]
        public void BuildTimesAsMicrosecondsFromMidnight(Type type)
        {
            var builder = new SchemaBuilder(temporalBehavior: TemporalBehavior.EpochMicroseconds);
            var schema = Assert.IsType<LongSchema>(builder.BuildSchema(type));
            Assert.IsType<MicrosecondTimeLogicalType>(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(TimeOnly))]
        public void BuildTimesAsMillisecondsFromMidnight(Type type)
        {
            var builder = new SchemaBuilder(temporalBehavior: TemporalBehavior.EpochMilliseconds);
            var schema = Assert.IsType<IntSchema>(builder.BuildSchema(type));
            Assert.IsType<MillisecondTimeLogicalType>(schema.LogicalType);
        }
#endif

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void BuildTimestampsAsIso8601Strings(Type type)
        {
            var builder = new SchemaBuilder(temporalBehavior: TemporalBehavior.Iso8601);
            var schema = Assert.IsType<StringSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void BuildTimestampsAsMicrosecondsFromEpoch(Type type)
        {
            var builder = new SchemaBuilder(temporalBehavior: TemporalBehavior.EpochMicroseconds);
            var schema = Assert.IsType<LongSchema>(builder.BuildSchema(type));
            Assert.IsType<MicrosecondTimestampLogicalType>(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void BuildTimestampsAsMillisecondsFromEpoch(Type type)
        {
            var builder = new SchemaBuilder(temporalBehavior: TemporalBehavior.EpochMilliseconds);
            var schema = Assert.IsType<LongSchema>(builder.BuildSchema(type));
            Assert.IsType<MillisecondTimestampLogicalType>(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(Uri))]
        public void BuildUris(Type type)
        {
            var schema = Assert.IsType<StringSchema>(builder.BuildSchema(type));
            Assert.Null(schema.LogicalType);
        }

        [Theory]
        [InlineData(typeof(Guid))]
        public void BuildUuids(Type type)
        {
            var schema = Assert.IsType<StringSchema>(builder.BuildSchema(type));
            Assert.IsType<UuidLogicalType>(schema.LogicalType);
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
