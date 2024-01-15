namespace Chr.Avro.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;
    using Xunit;

    public class MapSchemaShould
    {
        [Fact]
        public void SetValueSchema()
        {
            var schema = new MapSchema(new NullSchema());
            Assert.IsType<NullSchema>(schema.Value);

            schema.Value = new IntSchema();
            Assert.IsType<IntSchema>(schema.Value);
        }

        [Fact]
        public void ThrowWhenConstructedWithNullValueSchema()
        {
            Assert.Throws<ArgumentNullException>(() => new MapSchema(null));
        }

        [Fact]
        public void ThrowWhenValueSchemaIsNeverSet()
        {
            var schema = ReflectionExtensions.GetUninitializedInstance<MapSchema>();
            Assert.Throws<InvalidOperationException>(() => schema.Value);
        }

        [Fact]
        public void ThrowWhenValueSchemaIsSetToNull()
        {
            var schema = new MapSchema(new IntSchema());
            Assert.Throws<ArgumentNullException>(() => schema.Value = null);
            Assert.IsType<IntSchema>(schema.Value);
        }
    }
}
