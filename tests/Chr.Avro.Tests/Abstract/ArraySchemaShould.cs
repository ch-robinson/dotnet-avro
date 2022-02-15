namespace Chr.Avro.Tests
{
    using System;
    using System.Runtime.Serialization;
    using Chr.Avro.Abstract;
    using Xunit;

    public class ArraySchemaShould
    {
        [Fact]
        public void SetItemSchema()
        {
            var schema = new ArraySchema(new NullSchema());
            Assert.IsType<NullSchema>(schema.Item);

            schema.Item = new IntSchema();
            Assert.IsType<IntSchema>(schema.Item);
        }

        [Fact]
        public void ThrowWhenConstructedWithNullItemSchema()
        {
            Assert.Throws<ArgumentNullException>(() => new ArraySchema(null));
        }

        [Fact]
        public void ThrowWhenItemSchemaIsNeverSet()
        {
            var schema = (ArraySchema)FormatterServices.GetUninitializedObject(typeof(ArraySchema));
            Assert.Throws<InvalidOperationException>(() => schema.Item);
        }

        [Fact]
        public void ThrowWhenItemSchemaIsSetToNull()
        {
            var schema = new ArraySchema(new IntSchema());
            Assert.Throws<ArgumentNullException>(() => schema.Item = null);
            Assert.IsType<IntSchema>(schema.Item);
        }
    }
}
