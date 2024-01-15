#pragma warning disable IDE0060, xUnit1026 // unused parameters

namespace Chr.Avro.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Chr.Avro.Infrastructure;
    using Xunit;

    public class NamedSchemaShould
    {
        [Theory]
        [MemberData(nameof(IdentifierData.ValidNamespacedIdentifiers), MemberType = typeof(IdentifierData))]
        public void GetNameComponents(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema(full);
            Assert.Equal(full, schema.FullName);
            Assert.Equal(name, schema.Name);
            Assert.Equal(space, schema.Namespace);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.ValidNamespacedIdentifiers), MemberType = typeof(IdentifierData))]
        public void IgnoreDuplicateAliases(string full, string space, string name)
        {
            var duplicate = $"{name}_dupe";

            var schema = new ConcreteNamedSchema("test");
            Assert.Empty(schema.Aliases);

            schema.Aliases.Add(duplicate);
            Assert.Equal(new[] { duplicate }, schema.Aliases);

            schema.Aliases.Add(full);
            Assert.Equal(new[] { duplicate, full }, schema.Aliases);

            schema.Aliases.Add(duplicate);
            Assert.Equal(new[] { duplicate, full }, schema.Aliases);

            schema.Aliases = new[] { duplicate, duplicate, duplicate };
            Assert.Equal(new[] { duplicate }, schema.Aliases);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.ValidNamespacedIdentifiers), MemberType = typeof(IdentifierData))]
        public void SetFullName(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test")
            {
                FullName = full,
            };

            Assert.Equal(full, schema.FullName);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.ValidNamespacedIdentifiers), MemberType = typeof(IdentifierData))]
        public void SetNamespace(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test")
            {
                Namespace = space,
            };

            Assert.Equal(space == null ? "test" : $"{space}.test", schema.FullName);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.ValidNamespacedIdentifiers), MemberType = typeof(IdentifierData))]
        public void SetQualifiedName(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test.test")
            {
                Name = full,
            };

            Assert.Equal(space == null ? $"test.{name}" : full, schema.FullName);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.ValidNamespacedIdentifiers), MemberType = typeof(IdentifierData))]
        public void SetUnqualifiedName(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test")
            {
                Name = full,
            };

            Assert.Equal(full, schema.FullName);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.NamespacedIdentifiersWithInvalidNames), MemberType = typeof(IdentifierData))]
        [MemberData(nameof(IdentifierData.NamespacedIdentifiersWithInvalidNamespaces), MemberType = typeof(IdentifierData))]
        public void ThrowWhenConstructedWithInvalidSchemaName(string full, string space, string name)
        {
            Assert.Throws<InvalidNameException>(() => new ConcreteNamedSchema(full));
        }

        [Fact]
        public void ThrowWhenConstructedWithNullSchemaName()
        {
            Assert.Throws<ArgumentNullException>(() => new ConcreteNamedSchema(null));
        }

        [Fact]
        public void ThrowWhenAliasCollectionIsNeverSet()
        {
            var schema = ReflectionExtensions.GetUninitializedInstance<ConcreteNamedSchema>();
            Assert.Throws<InvalidOperationException>(() => schema.Aliases);
        }

        [Fact]
        public void ThrowWhenAliasCollectionIsSetToNull()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Aliases = null);
        }

        [Fact]
        public void ThrowWhenFullNameIsSetToNull()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.FullName = null);
            Assert.Equal("test", schema.FullName);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.NamespacedIdentifiersWithInvalidNames), MemberType = typeof(IdentifierData))]
        [MemberData(nameof(IdentifierData.NamespacedIdentifiersWithInvalidNamespaces), MemberType = typeof(IdentifierData))]
        public void ThrowWhenInvalidAliasIsAdded(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<InvalidNameException>(() => schema.Aliases.Add(full));
            Assert.Throws<InvalidNameException>(() => schema.Aliases = new[] { full });
        }

        [Fact]
        public void ThrowWhenNameIsNeverSet()
        {
            var schema = ReflectionExtensions.GetUninitializedInstance<ConcreteNamedSchema>();
            Assert.Throws<InvalidOperationException>(() => schema.FullName);
            Assert.Throws<InvalidOperationException>(() => schema.Name);
            Assert.Throws<InvalidOperationException>(() => schema.Namespace);
        }

        [Fact]
        public void ThrowWhenNameIsSetToNull()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Name = null);
            Assert.Equal("test", schema.FullName);
        }

        [Fact]
        public void ThrowWhenNullAliasIsAdded()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Aliases.Add(null));
            Assert.Throws<ArgumentNullException>(() => schema.Aliases = new string[] { null });
        }

        private class ConcreteNamedSchema : NamedSchema
        {
            public ConcreteNamedSchema(string name)
            : base(name)
            {
            }
        }
    }
}
