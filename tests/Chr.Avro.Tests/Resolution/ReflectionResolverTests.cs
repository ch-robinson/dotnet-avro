using Chr.Avro.Resolution;
using System;
using Xunit;

namespace Chr.Avro.Tests
{
    public class ReflectionResolverTests : CommonResolverTests<ReflectionResolver>
    {
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
            Assert.Equal(typeof(DuplicateEnum).Name, resolution.Name.Value);

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
            Assert.Equal(typeof(ExplicitEnum).Name, resolution.Name.Value);

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
            Assert.Equal(typeof(ImplicitEnum).Name, resolution.Name.Value);

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
        [InlineData(typeof(FlagEnum), false)]
        [InlineData(typeof(FlagEnum?), true)]
        public void ResolvesFlagEnums(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.True(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

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
    }
}
