namespace Chr.Avro.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;
    using Xunit;

    public class RecordSchemaShould
    {
        [Fact]
        public void IgnoresDuplicateFields()
        {
            var duplicate = new RecordField("duplicate", new IntSchema());
            var other = new RecordField("other", new IntSchema());

            var schema = new RecordSchema("test");
            Assert.Empty(schema.Fields);

            schema.Fields.Add(duplicate);
            Assert.Equal(new[] { duplicate }, schema.Fields);

            schema.Fields.Add(other);
            Assert.Equal(new[] { duplicate, other }, schema.Fields);

            schema.Fields.Add(duplicate);
            Assert.Equal(new[] { duplicate, other }, schema.Fields);

            schema.Fields = new[] { duplicate, duplicate, duplicate };
            Assert.Equal(new[] { duplicate }, schema.Fields);
        }

        [Fact]
        public void IsNamedSchema()
        {
            Assert.IsAssignableFrom<NamedSchema>(new RecordSchema("test"));
        }

        [Fact]
        public void SetsDocumentation()
        {
            var schema = new RecordSchema("test");
            Assert.Null(schema.Documentation);

            schema.Documentation = "documentation";
            Assert.Equal("documentation", schema.Documentation);

            schema.Documentation = null;
            Assert.Null(schema.Documentation);
        }

        [Fact]
        public void ThrowsWhenFieldCollectionIsNeverSet()
        {
            var schema = ReflectionExtensions.GetUninitializedInstance<RecordSchema>();
            Assert.Throws<InvalidOperationException>(() => schema.Fields);
        }

        [Fact]
        public void ThrowsWhenFieldCollectionIsSetToNull()
        {
            var schema = new RecordSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Fields = null);
        }

        [Fact]
        public void ThrowsWhenNullFieldIsAdded()
        {
            var schema = new RecordSchema("test");
            Assert.Throws<ArgumentNullException>(() => schema.Fields.Add(null));
            Assert.Throws<ArgumentNullException>(() => schema.Fields = new RecordField[] { null });
        }
    }
}
