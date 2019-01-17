using Chr.Avro.Resolution;
using System;
using Xunit;

namespace Chr.Avro.Tests
{
    public class DataContractResolverTests : CommonResolverTests<DataContractResolver>
    {
        [Fact]
        public void ResolvesClassesWithDataContractAttributes()
        {
            var resolution = Resolver.ResolveType<DataContractAnnotatedClass>() as RecordResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsNullable);
            Assert.Equal(typeof(DataContractAnnotatedClass), resolution.Type);

            Assert.True(resolution.Name.IsSetExplicitly);
            Assert.Equal("annotated", resolution.Name.Value);

            Assert.True(resolution.Namespace.IsSetExplicitly);
            Assert.Equal("chr.tests", resolution.Namespace.Value);

            Assert.Collection(resolution.Fields,
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal("AnnotatedDefaultField", f.Name.Value);
                },
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal("AnnotatedDefaultProperty", f.Name.Value);
                },
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal("ConflictingField", f.Name.Value);
                },
                f =>
                {
                    Assert.True(f.Name.IsSetExplicitly);
                    Assert.Equal("DifferentProperty", f.Name.Value);
                },
                f =>
                {
                    Assert.True(f.Name.IsSetExplicitly);
                    Assert.Equal("DifferentField", f.Name.Value);
                }
            );
        }
        
        [Fact]
        public void ResolvesClassesWithNonSerializedMemberAttributes()
        {
            var resolution = Resolver.ResolveType<DataContractNonAnnotatedClass>() as RecordResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsNullable);
            Assert.Equal(typeof(DataContractNonAnnotatedClass), resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(DataContractNonAnnotatedClass).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(DataContractNonAnnotatedClass).Namespace, resolution.Namespace.Value);

            Assert.Collection(resolution.Fields,
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedCustomField), f.Name.Value);
                },
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedCustomProperty), f.Name.Value);
                },
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedDefaultField), f.Name.Value);
                },
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedClass.AnnotatedDefaultProperty), f.Name.Value);
                },
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedClass.UnannotatedField), f.Name.Value);
                },
                f =>
                {
                    Assert.False(f.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedClass.UnannotatedProperty), f.Name.Value);
                }
            );
        }

        [Theory]
        [InlineData(typeof(DataContractAnnotatedEnum), false)]
        [InlineData(typeof(DataContractAnnotatedEnum?), true)]
        public void ResolvesEnumsWithDataContractAttributes(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

            Assert.True(resolution.Name.IsSetExplicitly);
            Assert.Equal("annotated", resolution.Name.Value);

            Assert.True(resolution.Namespace.IsSetExplicitly);
            Assert.Equal("chr.tests", resolution.Namespace.Value);

            Assert.Collection(resolution.Symbols,
                s =>
                {
                    Assert.Equal(nameof(DataContractAnnotatedEnum.Conflicting), s.Member.Name);
                    Assert.False(s.Name.IsSetExplicitly);
                    Assert.Equal("Conflicting", s.Name.Value);
                    Assert.Equal(DataContractAnnotatedEnum.Conflicting, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(DataContractAnnotatedEnum.Default), s.Member.Name);
                    Assert.False(s.Name.IsSetExplicitly);
                    Assert.Equal("Default", s.Name.Value);
                    Assert.Equal(DataContractAnnotatedEnum.Default, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(DataContractAnnotatedEnum.Custom), s.Member.Name);
                    Assert.True(s.Name.IsSetExplicitly);
                    Assert.Equal("Different", s.Name.Value);
                    Assert.Equal(DataContractAnnotatedEnum.Custom, s.Value);
                }
            );
        }

        [Theory]
        [InlineData(typeof(DataContractNonAnnotatedEnum), false)]
        [InlineData(typeof(DataContractNonAnnotatedEnum?), true)]
        public void ResolvesEnumsWithNonSerializedSymbolAttributes(Type type, bool isNullable)
        {
            var resolution = Resolver.ResolveType(type) as EnumResolution;

            Assert.NotNull(resolution);
            Assert.False(resolution.IsFlagEnum);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(type, resolution.Type);

            Assert.False(resolution.Name.IsSetExplicitly);
            Assert.Equal(typeof(DataContractNonAnnotatedEnum).Name, resolution.Name.Value);

            Assert.False(resolution.Namespace.IsSetExplicitly);
            Assert.Equal(typeof(DataContractNonAnnotatedEnum).Namespace, resolution.Namespace.Value);

            Assert.Collection(resolution.Symbols,
                s =>
                {
                    Assert.Equal(nameof(DataContractNonAnnotatedEnum.None), s.Member.Name);
                    Assert.False(s.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedEnum.None), s.Name.Value);
                    Assert.Equal(DataContractNonAnnotatedEnum.None, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(DataContractNonAnnotatedEnum.Default), s.Member.Name);
                    Assert.False(s.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedEnum.Default), s.Name.Value);
                    Assert.Equal(DataContractNonAnnotatedEnum.Default, s.Value);
                },
                s =>
                {
                    Assert.Equal(nameof(DataContractNonAnnotatedEnum.Custom), s.Member.Name);
                    Assert.False(s.Name.IsSetExplicitly);
                    Assert.Equal(nameof(DataContractNonAnnotatedEnum.Custom), s.Name.Value);
                    Assert.Equal(DataContractNonAnnotatedEnum.Custom, s.Value);
                }
            );
        }
    }
}
