namespace Chr.Avro.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Xunit;

    public class FixedSchemaShould
    {
        [Fact]
        public void SetSize()
        {
            var schema = new FixedSchema("test", 0);
            Assert.Equal(0, schema.Size);

            schema.Size = 4;
            Assert.Equal(4, schema.Size);
        }

        [Fact]
        public void ThrowWhenSizeIsSetToNegativeValue()
        {
            var schema = new FixedSchema("test", 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => schema.Size = -1);
        }
    }
}
