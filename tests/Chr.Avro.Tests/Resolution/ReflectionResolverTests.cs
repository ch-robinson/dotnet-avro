using Chr.Avro.Resolution;
using System;
using Xunit;

namespace Chr.Avro.Tests
{
    public class ReflectionResolverTests : CommonResolverTests<ReflectionResolver>
    {
        protected override ReflectionResolver Resolver => new ReflectionResolver();

        [Theory]
        [InlineData(typeof(LongEnum), false, true, 64)]
        [InlineData(typeof(LongEnum?), true, true, 64)]
        [InlineData(typeof(LongFlagEnum), false, true, 64)]
        [InlineData(typeof(LongFlagEnum?), true, true, 64)]
        [InlineData(typeof(UIntEnum), false, false, 32)]
        [InlineData(typeof(UIntEnum?), true, false, 32)]
        [InlineData(typeof(UIntFlagEnum), false, false, 32)]
        [InlineData(typeof(UIntFlagEnum?), true, false, 32)]
        public void ResolvesEnumsAsUnderlyingIntegralTypes(Type type, bool isNullable, bool isSigned, int size)
        {
            var resolver = new ReflectionResolver(resolveUnderlyingEnumTypes: true);
            var resolution = resolver.ResolveType(type) as IntegerResolution;

            Assert.NotNull(resolution);
            Assert.Equal(isNullable, resolution.IsNullable);
            Assert.Equal(isSigned, resolution.IsSigned);
            Assert.Equal(size, resolution.Size);
            Assert.Equal(type, resolution.Type);
        }

        [Theory]
        [InlineData(typeof(object))]
        public void ResolvesReferenceTypesAsNullable(Type type)
        {
            var resolver = new ReflectionResolver(resolveReferenceTypesAsNullable: true);
            var resolution = resolver.ResolveType(type);

            Assert.True(resolution.IsNullable);
            Assert.Equal(type, resolution.Type);
        }

        [Fact]
        public void ResolvesConstructors()
        {
            var resolver = new ReflectionResolver();
            var resolution = resolver.ResolveType<ConstructorClass>() as RecordResolution;

            Assert.NotEmpty(resolution.Constructors);
            Assert.Collection(resolution.Constructors,
                c =>
                {
                    Assert.NotEmpty(c.Parameters);
                    Assert.Collection(c.Parameters,
                        p =>
                        {
                            Assert.Equal("fieldA", p.Name.Value);
                            Assert.False(p.Parameter.IsOptional);
                            Assert.Equal(typeof(int), p.Type);
                        },
                        p =>
                        {
                            Assert.Equal("fieldB", p.Name.Value);
                            Assert.True(p.Parameter.IsOptional);
                            Assert.Equal(typeof(string), p.Type);
                        });
                });
        }
    }
}
