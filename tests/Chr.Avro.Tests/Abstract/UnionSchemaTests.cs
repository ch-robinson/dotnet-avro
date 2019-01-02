using Chr.Avro.Abstract;
using System;
using System.Runtime.Serialization;
using Xunit;

namespace Chr.Avro.Tests
{
    public class UnionSchemaTests
    {
        [Fact]
        public void AllowsMultipleNamedSchemas()
        {
            var schema = new UnionSchema();
            schema.Schemas.Add(new RecordSchema("RecordA"));
            schema.Schemas.Add(new RecordSchema("RecordB"));
            schema.Schemas.Add(new RecordSchema("RecordA"));

            Assert.Collection(schema.Schemas,
                s => Assert.True(s is RecordSchema),
                s => Assert.True(s is RecordSchema),
                s => Assert.True(s is RecordSchema)
            );
        }

        [Fact]
        public void IsSchema()
        {
            Assert.IsAssignableFrom<Schema>(new UnionSchema());
        }

        [Fact]
        public void SetsSchemas()
        {
            var schema = new UnionSchema();
            Assert.Empty(schema.Schemas);

            schema.Schemas = new[] { new FloatSchema() };
            Assert.Collection(schema.Schemas, s => Assert.True(s is FloatSchema));
        }

        [Fact]
        public void ThrowsWhenDuplicateSchemaIsAdded()
        {
            Assert.Throws<InvalidSchemaException>(() =>
            {
                var schema = new UnionSchema();
                schema.Schemas.Add(new BooleanSchema());
                schema.Schemas.Add(new BooleanSchema());
            });

            Assert.Throws<InvalidSchemaException>(() =>
            {
                var schema = new UnionSchema(new[]
                {
                    new ArraySchema(new IntSchema()),
                    new ArraySchema(new StringSchema()),
                });
            });
        }

        [Fact]
        public void ThrowsWhenNullSchemaIsAdded()
        {
            var schema = new UnionSchema();
            Assert.Throws<ArgumentNullException>(() => schema.Schemas.Add(null));
            Assert.Throws<ArgumentNullException>(() => schema.Schemas = new Schema[] { null });
        }

        [Fact]
        public void ThrowsWhenSchemaCollectionIsNeverSet()
        {
            var schema = (UnionSchema)FormatterServices.GetUninitializedObject(typeof(UnionSchema));
            Assert.Throws<InvalidOperationException>(() => schema.Schemas);
        }

        [Fact]
        public void ThrowsWhenSchemaCollectionIsSetToNull()
        {
            var schema = new UnionSchema();
            Assert.Throws<ArgumentNullException>(() => schema.Schemas = null);
        }

        [Fact]
        public void ThrowsWhenUnionSchemaIsAdded()
        {
            Assert.Throws<InvalidSchemaException>(() =>
            {
                var schema = new UnionSchema();
                schema.Schemas.Add(new UnionSchema(new Schema[] { new NullSchema(), new IntSchema() }));
            });
        }
    }
}
