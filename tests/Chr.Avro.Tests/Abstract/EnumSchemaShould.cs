namespace Chr.Avro.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Chr.Avro.Infrastructure;
    using Xunit;

    public class EnumSchemaShould
    {
        [Theory]
        [MemberData(nameof(IdentifierData.ValidIdentifiers), MemberType = typeof(IdentifierData))]
        public void IgnoreDuplicateSymbols(string symbol)
        {
            var duplicate = $"{symbol}_dupe";

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
        public void SetDocumentation()
        {
            var schema = new EnumSchema("test");
            Assert.Null(schema.Documentation);

            schema.Documentation = "documentation";
            Assert.Equal("documentation", schema.Documentation);

            schema.Documentation = null;
            Assert.Null(schema.Documentation);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.InvalidIdentifiers), MemberType = typeof(IdentifierData))]
        public void ThrowWhenInvalidSymbolIsAdded(string symbol)
        {
            var schema = new EnumSchema("test");
            Assert.Throws<InvalidSymbolException>(() => schema.Symbols.Add(symbol));
            Assert.Throws<InvalidSymbolException>(() => schema.Symbols = new[] { symbol });
        }

        [Fact]
        public void ThrowWhenNullSymbolIsAdded()
        {
            var schema = new EnumSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Symbols.Add(null));
            Assert.Throws<ArgumentNullException>(() => schema.Symbols = new string[] { null });
        }

        [Fact]
        public void ThrowWhenSymbolCollectionIsNeverSet()
        {
            var schema = ReflectionExtensions.GetUninitializedInstance<EnumSchema>();
            Assert.Throws<InvalidOperationException>(() => schema.Symbols);
        }

        [Fact]
        public void ThrowWhenSymbolCollectionIsSetToNull()
        {
            var schema = new EnumSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Aliases = null);
        }
    }
}
