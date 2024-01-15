namespace Chr.Avro.Tests
{
    using System;
    using System.Runtime.Serialization;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;
    using Xunit;

    public class UnionSchemaShould
    {
        [Fact]
        public void AllowMultipleNamedSchemas()
        {
            var schema = new UnionSchema();
            schema.Schemas.Add(new RecordSchema("RecordA"));
            schema.Schemas.Add(new RecordSchema("RecordB"));
            schema.Schemas.Add(new RecordSchema("RecordA"));

            Assert.Collection(
                schema.Schemas,
                s => Assert.True(s is RecordSchema),
                s => Assert.True(s is RecordSchema),
                s => Assert.True(s is RecordSchema));
        }

        [Fact]
        public void SetSchemas()
        {
            var schema = new UnionSchema();
            Assert.Empty(schema.Schemas);

            schema.Schemas = new[] { new FloatSchema() };
            Assert.Collection(schema.Schemas, s => Assert.True(s is FloatSchema));
        }

        [Fact]
        public void ThrowWhenDuplicateSchemaIsAdded()
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
        public void ThrowWhenNullSchemaIsAdded()
        {
            var schema = new UnionSchema();
            Assert.Throws<ArgumentNullException>(() => schema.Schemas.Add(null));
            Assert.Throws<ArgumentNullException>(() => schema.Schemas = new Schema[] { null });
        }

        [Fact]
        public void ThrowWhenSchemaCollectionIsNeverSet()
        {
            var schema = ReflectionExtensions.GetUninitializedInstance<UnionSchema>();
            Assert.Throws<InvalidOperationException>(() => schema.Schemas);
        }

        [Fact]
        public void ThrowWhenSchemaCollectionIsSetToNull()
        {
            var schema = new UnionSchema();
            Assert.Throws<ArgumentNullException>(() => schema.Schemas = null);
        }

        [Fact]
        public void ThrowWhenUnionSchemaIsAdded()
        {
            Assert.Throws<InvalidSchemaException>(() =>
            {
                var schema = new UnionSchema();
                schema.Schemas.Add(new UnionSchema(new Schema[] { new NullSchema(), new IntSchema() }));
            });
        }
    }
}
