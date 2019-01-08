using Chr.Avro.Resolution;
using System;
using System.Collections.Generic;
using Xunit;

namespace Chr.Avro.Tests
{
    public abstract class CommonResolverTests<T> where T : ReflectionResolver, new()
    {
        protected readonly T Resolver;

        public CommonResolverTests()
        {
            Resolver = new T();
        }

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
            Assert.Throws<UnsupportedTypeException>(() => Resolver.ResolveType(type));
        }
    }
}
