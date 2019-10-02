using Chr.Avro.Resolution;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Tests
{
    public abstract class CommonResolverTests<T> where T : ITypeResolver
    {
        protected abstract T Resolver { get; }

        [Theory]
        [InlineData(typeof(ICollection<string>), typeof(string), false)]
        [InlineData(typeof(IEnumerable<double>), typeof(double), false)]
        [InlineData(typeof(IList<bool>), typeof(bool), false)]
        [InlineData(typeof(List<object>), typeof(object), false)]
        [InlineData(typeof(List<List<object>>), typeof(List<object>), false)]
        [InlineData(typeof(int[]), typeof(int), false)]
        [InlineData(typeof(int[][]), typeof(int[]), false)]
        public void ResolvesArrays(Type type, Type itemType, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as ArrayResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(itemType, resolution.ItemType);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(bool), false)]
        [InlineData(typeof(bool?), true)]
        public void ResolvesBooleans(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as BooleanResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(byte[]), false)]
        public void ResolvesByteArrays(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as ByteArrayResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
        }
        [Fact]
        public void ResolvesClassesWithNoFields()
        {
            var resolution = Resolver.ResolveType<EmptyClass>() as RecordResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsNullable);
            Assert.Equal(typeof(EmptyClass), resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(EmptyClass).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(EmptyClass).Namespace, resolution.Namespace.Value);

            Assert.Empty(resolution.Fields);
        }

        [Theory]
        [InlineData(typeof(decimal), false, 29, 14)]
        [InlineData(typeof(decimal?), true, 29, 14)]
        public void ResolvesDecimals(Type type, bool isNullable, int precision, int scale)
        {
            var resolution = Resolver.ResolveType(type) as DecimalResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(precision, resolution.Precision);
            Assert.Equal(scale, resolution.Scale);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(TimeSpan), false, 1e-7)]
        [InlineData(typeof(TimeSpan?), true, 1e-7)]
        public void ResolvesDurations(Type type, bool isNullable, double precision)
        {
            var resolution = Resolver.ResolveType(type) as DurationResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal((decimal)precision, resolution.Precision);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(DuplicateEnum), false)]
        [InlineData(typeof(DuplicateEnum?), true)]
        public void ResolvesEnumsWithDuplicateValues(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(DuplicateEnum).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(DuplicateEnum).Namespace, resolution.Namespace.Value);

            Assert.All(resolution.Symbols, s => Assert.False(s.Name.IsSetExplicitly));
            Assert.Collection(resolution.Symbols,
                s =>
                {
                    Assert.Equal(nameof(DuplicateEnum.A), s.Member.Name);
                    Assert.Equal(nameof(DuplicateEnum.A), s.Name.Value);
                    Assert.Equal(DuplicateEnum.A, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(DuplicateEnum.B), s.Member.Name);
                    Assert.Equal(nameof(DuplicateEnum.B), s.Name.Value);
                    Assert.Equal(DuplicateEnum.B, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(DuplicateEnum.C), s.Member.Name);
                    Assert.Equal(nameof(DuplicateEnum.C), s.Name.Value);
                    Assert.Equal(DuplicateEnum.C, s.Value);
                }
            );
        }

        [Theory]
        [InlineData(typeof(ExplicitEnum), false)]
        [InlineData(typeof(ExplicitEnum?), true)]
        public void ResolvesEnumsWithExplicitValues(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(ExplicitEnum).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(ExplicitEnum).Namespace, resolution.Namespace.Value);

            Assert.All(resolution.Symbols, s => Assert.False(s.Name.IsSetExplicitly));
            Assert.Collection(resolution.Symbols,
                s =>
                {
                    Assert.Equal(nameof(ExplicitEnum.Third), s.Member.Name);
                    Assert.Equal(nameof(ExplicitEnum.Third), s.Name.Value);
                    Assert.Equal(ExplicitEnum.Third, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ExplicitEnum.First), s.Member.Name);
                    Assert.Equal(nameof(ExplicitEnum.First), s.Name.Value);
                    Assert.Equal(ExplicitEnum.First, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ExplicitEnum.None), s.Member.Name);
                    Assert.Equal(nameof(ExplicitEnum.None), s.Name.Value);
                    Assert.Equal(ExplicitEnum.None, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ExplicitEnum.Second), s.Member.Name);
                    Assert.Equal(nameof(ExplicitEnum.Second), s.Name.Value);
                    Assert.Equal(ExplicitEnum.Second, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ExplicitEnum.Fourth), s.Member.Name);
                    Assert.Equal(nameof(ExplicitEnum.Fourth), s.Name.Value);
                    Assert.Equal(ExplicitEnum.Fourth, s.Value);
                }
            );
        }

        [Theory]
        [InlineData(typeof(ImplicitEnum), false)]
        [InlineData(typeof(ImplicitEnum?), true)]
        public void ResolvesEnumsWithImplicitValues(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(ImplicitEnum).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(ImplicitEnum).Namespace, resolution.Namespace.Value);

            Assert.All(resolution.Symbols, s => Assert.False(s.Name.IsSetExplicitly));
            Assert.Collection(resolution.Symbols,
                s =>
                {
                    Assert.Equal(nameof(ImplicitEnum.None), s.Member.Name);
                    Assert.Equal(nameof(ImplicitEnum.None), s.Name.Value);
                    Assert.Equal(ImplicitEnum.None, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ImplicitEnum.First), s.Member.Name);
                    Assert.Equal(nameof(ImplicitEnum.First), s.Name.Value);
                    Assert.Equal(ImplicitEnum.First, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ImplicitEnum.Second), s.Member.Name);
                    Assert.Equal(nameof(ImplicitEnum.Second), s.Name.Value);
                    Assert.Equal(ImplicitEnum.Second, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ImplicitEnum.Third), s.Member.Name);
                    Assert.Equal(nameof(ImplicitEnum.Third), s.Name.Value);
                    Assert.Equal(ImplicitEnum.Third, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(ImplicitEnum.Fourth), s.Member.Name);
                    Assert.Equal(nameof(ImplicitEnum.Fourth), s.Name.Value);
                    Assert.Equal(ImplicitEnum.Fourth, s.Value);
                }
            );
        }

        [Theory]
        [InlineData(typeof(EmptyEnum), false)]
        [InlineData(typeof(EmptyEnum?), true)]
        public void ResolvesEnumsWithNoSymbols(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(EmptyEnum).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(EmptyEnum).Namespace, resolution.Namespace.Value);

            Assert.Empty(resolution.Symbols);
        }

        [Theory]
        [InlineData(typeof(FlagEnum), typeof(int), false)]
        [InlineData(typeof(FlagEnum?), typeof(int), true)]
        public void ResolvesFlagEnumsWithDefaultUnderlyingType(Type type, Type underlyingType, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.True(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
            Assert.Equal(underlyingType, resolution.UnderlyingType);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(FlagEnum).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(FlagEnum).Namespace, resolution.Namespace.Value);

            Assert.All(resolution.Symbols, s => Assert.False(s.Name.IsSetExplicitly));
            Assert.Collection(resolution.Symbols,
                s =>
                {
                    Assert.Equal(nameof(FlagEnum.None), s.Member.Name);
                    Assert.Equal(nameof(FlagEnum.None), s.Name.Value);
                    Assert.Equal(FlagEnum.None, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(FlagEnum.First), s.Member.Name);
                    Assert.Equal(nameof(FlagEnum.First), s.Name.Value);
                    Assert.Equal(FlagEnum.First, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(FlagEnum.Second), s.Member.Name);
                    Assert.Equal(nameof(FlagEnum.Second), s.Name.Value);
                    Assert.Equal(FlagEnum.Second, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(FlagEnum.Third), s.Member.Name);
                    Assert.Equal(nameof(FlagEnum.Third), s.Name.Value);
                    Assert.Equal(FlagEnum.Third, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(FlagEnum.Fourth), s.Member.Name);
                    Assert.Equal(nameof(FlagEnum.Fourth), s.Name.Value);
                    Assert.Equal(FlagEnum.Fourth, s.Value);
                }
            );
        }

        [Theory]
        [InlineData(typeof(LongFlagEnum), typeof(long), false)]
        [InlineData(typeof(LongFlagEnum?), typeof(long), true)]
        public void ResolvesFlagEnumsWithOtherUnderlyingTypes(Type type, Type underlyingType, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.True(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
            Assert.Equal(underlyingType, resolution.UnderlyingType);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(LongFlagEnum).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(LongFlagEnum).Namespace, resolution.Namespace.Value);

            Assert.Empty(resolution.Symbols);
        }

        [Theory]
        [InlineData(typeof(double), false, 16)]
        [InlineData(typeof(double?), true, 16)]
        [InlineData(typeof(float), false, 8)]
        [InlineData(typeof(float?), true, 8)]
        public void ResolvesFloatingPoints(Type type, bool isNullable, int size)
        {
            var resolution = Resolver.ResolveType(type) as FloatingPointResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(size, resolution.Size);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(byte), false, false, 8)]
        [InlineData(typeof(byte?), true, false, 8)]
        [InlineData(typeof(char), false, false, 16)]
        [InlineData(typeof(char?), true, false, 16)]
        [InlineData(typeof(int), false, true, 32)]
        [InlineData(typeof(int?), true, true, 32)]
        [InlineData(typeof(long), false, true, 64)]
        [InlineData(typeof(long?), true, true, 64)]
        [InlineData(typeof(sbyte), false, true, 8)]
        [InlineData(typeof(sbyte?), true, true, 8)]
        [InlineData(typeof(short), false, true, 16)]
        [InlineData(typeof(short?), true, true, 16)]
        [InlineData(typeof(uint), false, false, 32)]
        [InlineData(typeof(uint?), true, false, 32)]
        [InlineData(typeof(ulong), false, false, 64)]
        [InlineData(typeof(ulong?), true, false, 64)]
        [InlineData(typeof(ushort), false, false, 16)]
        [InlineData(typeof(ushort?), true, false, 16)]
        public void ResolvesIntegers(Type type, bool isNullable, bool isSigned, int size)
        {
            var resolution = Resolver.ResolveType(type) as IntegerResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(isSigned, resolution.IsSigned);
            Assert.Equal(size, resolution.Size);
            Assert.Equal(type, resolution.Type);
        }

        [Fact]
        public void ResolvesInterfacesWithNoFields()
        {
            var resolution = Resolver.ResolveType<IEmptyInterface>() as RecordResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsNullable);
            Assert.Equal(typeof(IEmptyInterface), resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(IEmptyInterface).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(IEmptyInterface).Namespace, resolution.Namespace.Value);

            Assert.Empty(resolution.Fields);
        }

        [Theory]
        [InlineData(typeof(IEnumerable<KeyValuePair<int, string>>), typeof(int), typeof(string), false)]
        [InlineData(typeof(IDictionary<int, string>), typeof(int), typeof(string), false)]
        [InlineData(typeof(Dictionary<int, string>), typeof(int), typeof(string), false)]
        public void ResolvesMaps(Type type, Type keyType, Type valueType, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as MapResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(keyType, resolution.KeyType);
            Assert.Equal(type, resolution.Type);
            Assert.Equal(valueType, resolution.ValueType);
        }

        [Fact]
        public void ResolvesPublicClassFieldsAndProperties()
        {
            var resolution = Resolver.ResolveType<VisibilityClass>() as RecordResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsNullable);
            Assert.Equal(typeof(VisibilityClass), resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(VisibilityClass).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(VisibilityClass).Namespace, resolution.Namespace.Value);

            Assert.All(resolution.Fields, f => Assert.False(f.Name.IsSetExplicitly));
            Assert.All(resolution.Fields, f => Assert.Equal(typeof(int), f.Type));
            Assert.Collection(resolution.Fields,
                f => Assert.Equal(nameof(VisibilityClass.PublicField), f.Name.Value),
                f => Assert.Equal(nameof(VisibilityClass.PublicProperty), f.Name.Value)
            );
        }

        [Fact]
        public void ResolvesPublicInterfaceProperties()
        {
            var resolution = Resolver.ResolveType<IVisibilityInterface>() as RecordResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsNullable);
            Assert.Equal(typeof(IVisibilityInterface), resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(IVisibilityInterface).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(IVisibilityInterface).Namespace, resolution.Namespace.Value);

            Assert.All(resolution.Fields, f => Assert.False(f.Name.IsSetExplicitly));
            Assert.All(resolution.Fields, f => Assert.Equal(typeof(int), f.Type));
            Assert.Collection(resolution.Fields,
                f => Assert.Equal(nameof(IVisibilityInterface.PublicProperty), f.Name.Value)
            );
        }

        [Theory]
        [InlineData(typeof(string), false)]
        public void ResolvesStrings(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as StringResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(EmptyStruct), false)]
        [InlineData(typeof(EmptyStruct?), true)]
        public void ResolvesStructsWithNoFields(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as RecordResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(EmptyStruct).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(EmptyStruct).Namespace, resolution.Namespace.Value);

            Assert.Empty(resolution.Fields);
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        public void ResolvesTimestamps(Type type)
        {
            var resolution = Resolver.ResolveType(type) as TimestampResolution;

            Assert.NotNull(resolution);
            Assert.Equal(0.0000001m, resolution.Precision);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(Uri), false)]
        public void ResolvesUris(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as UriResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(Guid), false, 2, 4)]
        [InlineData(typeof(Guid?), true, 2, 4)]
        public void ResolvesUuids(Type type, bool isNullable, int variant, int version)
        {
            var resolution = Resolver.ResolveType(type) as UuidResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
            Assert.Equal(variant, resolution.Variant);
            Assert.Equal(version, resolution.Version);
        }

        [Theory]
        [InlineData(typeof(IntPtr))]
        [InlineData(typeof(int[,]))]
        public void ThrowsWhenNoCaseMatches(Type type)
        {
            Assert.Throws<AggregateException>(() => Resolver.ResolveType(type));
        }
    }
}
