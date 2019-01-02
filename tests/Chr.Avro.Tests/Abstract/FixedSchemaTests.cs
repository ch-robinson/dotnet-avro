using Chr.Avro.Abstract;
using System;
using Xunit;

namespace Chr.Avro.Tests
{
    public class FixedSchemaTests
    {
        [Fact]
        public void IsNamedSchema()
        {
            Assert.IsAssignableFrom<NamedSchema>(new FixedSchema("test", 0));
        }

        [Fact]
        public void SetsSize()
        {
            var schema = new FixedSchema("test", 0);
            Assert.Equal(0, schema.Size);

            schema.Size = 4;
            Assert.Equal(4, schema.Size);
        }

        [Fact]
        public void ThrowsWhenSizeIsSetToNegativeValue()
        {
            var schema = new FixedSchema("test", 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => schema.Size = -1);
        }
    }
}
