using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Xunit;

namespace Chr.Avro.Tests
{
    public class EnumSchemaTests
    {
        [Theory]
        [MemberData(nameof(ValidSymbolData))]
        public void IgnoresDuplicateSymbols(string symbol)
        {
            var duplicate = "DUPLICATE";

            var schema = new EnumSchema("test");
            Assert.Empty(schema.Symbols);

            schema.Symbols.Add(duplicate);
            Assert.Equal(new[] { duplicate }, schema.Symbols);

            schema.Symbols.Add(symbol);
            Assert.Equal(new[] { duplicate, symbol }, schema.Symbols);

            schema.Symbols.Add(duplicate);
            Assert.Equal(new[] { duplicate, symbol }, schema.Symbols);

            schema.Symbols = new[] { duplicate, duplicate, duplicate };
            Assert.Equal(new[] { duplicate }, schema.Symbols);
        }

        [Fact]
        public void IsNamedSchema()
        {
            Assert.IsAssignableFrom<NamedSchema>(new EnumSchema("test"));
        }

        [Fact]
        public void SetsDocumentation()
        {
            var schema = new EnumSchema("test");
            Assert.Null(schema.Documentation);

            schema.Documentation = "documentation";
            Assert.Equal("documentation", schema.Documentation);

            schema.Documentation = null;
            Assert.Null(schema.Documentation);
        }

        [Theory]
        [MemberData(nameof(InvalidSymbolData))]
        public void ThrowsWhenInvalidSymbolIsAdded(string symbol)
        {
            var schema = new EnumSchema("test");
            Assert.Throws<InvalidSymbolException>(() => schema.Symbols.Add(symbol));
            Assert.Throws<InvalidSymbolException>(() => schema.Symbols = new[] { symbol });
        }

        [Fact]
        public void ThrowsWhenNullSymbolIsAdded()
        {
            var schema = new EnumSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Symbols.Add(null));
            Assert.Throws<ArgumentNullException>(() => schema.Symbols = new string[] { null });
        }

        [Fact]
        public void ThrowsWhenSymbolCollectionIsNeverSet()
        {
            var schema = (EnumSchema)FormatterServices.GetUninitializedObject(typeof(EnumSchema));
            Assert.Throws<InvalidOperationException>(() => schema.Symbols);
        }

        [Fact]
        public void ThrowsWhenSymbolCollectionIsSetToNull()
        {
            var schema = new EnumSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Aliases = null);
        }

        public static IEnumerable<object[]> InvalidSymbolData => new List<object[]>
        {
            new object[] { "" },
            new object[] { "1symbol" },
            new object[] { "space space" },
            new object[] { "kebab-case" },
            new object[] { "namespace.symbol" },
        };

        public static IEnumerable<object[]> ValidSymbolData => new List<object[]>
        {
            new object[] { "lowercase" },
            new object[] { "UPPERCASE" },
            new object[] { "snake_case" },
            new object[] { "_" },
            new object[] { "symbol1" },
        };
    }
}
