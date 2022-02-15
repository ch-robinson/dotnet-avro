namespace Chr.Avro.Tests
{
    using System;
    using System.Runtime.Serialization;
    using Chr.Avro.Abstract;
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
            var schema = (MapSchema)FormatterServices.GetUninitializedObject(typeof(MapSchema));
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
