using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Xunit;

#pragma warning disable xUnit1026 // unused parameters

namespace Chr.Avro.Tests
{
    public class NamedSchemaTests
    {
        [Theory]
        [MemberData(nameof(ValidFullnameData))]
        public void GetsNameComponents(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema(full);
            Assert.Equal(full, schema.FullName);
            Assert.Equal(name, schema.Name);
            Assert.Equal(space, schema.Namespace);
        }

        [Theory]
        [MemberData(nameof(ValidFullnameData))]
        public void IgnoresDuplicateAliases(string full, string space, string name)
        {
            var duplicate = "duplicate";

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
        [MemberData(nameof(ValidFullnameData))]
        public void SetsFullName(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test")
            {
                FullName = full
            };

            Assert.Equal(full, schema.FullName);
        }

        [Theory]
        [MemberData(nameof(ValidFullnameData))]
        public void SetsNamespace(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test")
            {
                Namespace = space
            };

            Assert.Equal(space == null ? "test" : $"{space}.test", schema.FullName);
        }

        [Theory]
        [MemberData(nameof(ValidFullnameData))]
        public void SetsQualifiedName(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test.test")
            {
                Name = full
            };

            Assert.Equal(space == null ? $"test.{name}" : full, schema.FullName);
        }

        [Theory]
        [MemberData(nameof(ValidFullnameData))]
        public void SetsUnqualifiedName(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test")
            {
                Name = full
            };

            Assert.Equal(full, schema.FullName);
        }

        [Theory]
        [MemberData(nameof(InvalidNameData))]
        [MemberData(nameof(InvalidNamespaceData))]
        public void ThrowsWhenConstructedWithInvalidSchemaName(string full, string space, string name)
        {
            Assert.Throws<InvalidNameException>(() => new ConcreteNamedSchema(full));
        }

        [Fact]
        public void ThrowsWhenConstructedWithNullSchemaName()
        {
            Assert.Throws<ArgumentNullException>(() => new ConcreteNamedSchema(null));
        }

        [Fact]
        public void ThrowsWhenAliasCollectionIsNeverSet()
        {
            var schema = (ConcreteNamedSchema)FormatterServices.GetUninitializedObject(typeof(ConcreteNamedSchema));
            Assert.Throws<InvalidOperationException>(() => schema.Aliases);
        }

        [Fact]
        public void ThrowsWhenAliasCollectionIsSetToNull()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Aliases = null);
        }

        [Fact]
        public void ThrowsWhenFullNameIsSetToNull()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.FullName = null);
            Assert.Equal("test", schema.FullName);
        }

        [Theory]
        [MemberData(nameof(InvalidNameData))]
        [MemberData(nameof(InvalidNamespaceData))]
        public void ThrowsWhenInvalidAliasIsAdded(string full, string space, string name)
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<InvalidNameException>(() => schema.Aliases.Add(full));
            Assert.Throws<InvalidNameException>(() => schema.Aliases = new[] { full });
        }

        [Fact]
        public void ThrowsWhenNameIsNeverSet()
        {
            var schema = (ConcreteNamedSchema)FormatterServices.GetUninitializedObject(typeof(ConcreteNamedSchema));
            Assert.Throws<InvalidOperationException>(() => schema.FullName);
            Assert.Throws<InvalidOperationException>(() => schema.Name);
            Assert.Throws<InvalidOperationException>(() => schema.Namespace);
        }

        [Fact]
        public void ThrowsWhenNameIsSetToNull()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Name = null);
            Assert.Equal("test", schema.FullName);
        }

        [Fact]
        public void ThrowsWhenNullAliasIsAdded()
        {
            var schema = new ConcreteNamedSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Aliases.Add(null));
            Assert.Throws<ArgumentNullException>(() => schema.Aliases = new string[] { null });
        }

        public static IEnumerable<object[]> InvalidNameData => new List<object[]>
        {
            new object[] { "", null, "" },
            new object[] { "space space", null, "space space" },
            new object[] { "kebab-case", null, "kebab-case" },
            new object[] { "1nvalid", null, "1nvalid" },
            new object[] { "inv?l!d", null, "inv?l!d" },
            new object[] { "name.", "name", "" },
            new object[] { "name.name.", "name.name", "" },
        };

        public static IEnumerable<object[]> InvalidNamespaceData => new List<object[]>
        {
            new object[] { ".name", "", "name" },
            new object[] { "..name", ".", "name" },
            new object[] { "space space.name", "space space", "name" },
            new object[] { "2x.4y.6z.name", "2x.4y.6z", "" },
        };

        public static IEnumerable<object[]> ValidFullnameData => new List<object[]>
        {
            new object[] { "lowercase", null, "lowercase" },
            new object[] { "PascalCase", null, "PascalCase" },
            new object[] { "snake_case", null, "snake_case" },
            new object[] { "_", null, "_" },
            new object[] { "a8", null, "a8" },
            new object[] { "_6", null, "_6" },
            new object[] { "namespaced.name", "namespaced", "name" },
            new object[] { "deeper.namespaced.name", "deeper.namespaced", "name" },
            new object[] { "_._", "_", "_" },
            new object[] { "x2.y4.z6.a8", "x2.y4.z6", "a8" },
        };

        private class ConcreteNamedSchema : NamedSchema
        {
            public ConcreteNamedSchema(string name) : base(name) { }
        }
    }
}
