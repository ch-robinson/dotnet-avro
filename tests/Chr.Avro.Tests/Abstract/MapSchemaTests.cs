using Chr.Avro.Abstract;
using System;
using System.Runtime.Serialization;
using Xunit;

namespace Chr.Avro.Tests
{
    public class MapSchemaTests
    {
        [Fact]
        public void IsSchema()
        {
            Assert.IsAssignableFrom<Schema>(new MapSchema(new IntSchema()));
        }

        [Fact]
        public void SetsValueSchema()
        {
            var schema = new MapSchema(new NullSchema());
            Assert.IsType<NullSchema>(schema.Value);

            schema.Value = new IntSchema();
            Assert.IsType<IntSchema>(schema.Value);
        }

        [Fact]
        public void ThrowsWhenConstructedWithNullValueSchema()
        {
            Assert.Throws<ArgumentNullException>(() => new MapSchema(null));
        }

        [Fact]
        public void ThrowsWhenValueSchemaIsNeverSet()
        {
            var schema = (MapSchema)FormatterServices.GetUninitializedObject(typeof(MapSchema));
            Assert.Throws<InvalidOperationException>(() => schema.Value);
        }

        [Fact]
        public void ThrowsWhenValueSchemaIsSetToNull()
        {
            var schema = new MapSchema(new IntSchema());
            Assert.Throws<ArgumentNullException>(() => schema.Value = null);
            Assert.IsType<IntSchema>(schema.Value);
        }
    }
}
