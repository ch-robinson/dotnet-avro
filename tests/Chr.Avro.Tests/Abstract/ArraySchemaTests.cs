using Chr.Avro.Abstract;
using System;
using System.Runtime.Serialization;
using Xunit;

namespace Chr.Avro.Tests
{
    public class ArraySchemaTests
    {
        [Fact]
        public void IsSchema()
        {
            Assert.IsAssignableFrom<Schema>(new ArraySchema(new IntSchema()));
        }

        [Fact]
        public void SetsItemSchema()
        {
            var schema = new ArraySchema(new NullSchema());
            Assert.IsType<NullSchema>(schema.Item);

            schema.Item = new IntSchema();
            Assert.IsType<IntSchema>(schema.Item);
        }


        [Fact]
        public void ThrowsWhenConstructedWithNullItemSchema()
        {
            Assert.Throws<ArgumentNullException>(() => new ArraySchema(null));
        }

        [Fact]
        public void ThrowsWhenItemSchemaIsNeverSet()
        {
            var schema = (ArraySchema)FormatterServices.GetUninitializedObject(typeof(ArraySchema));
            Assert.Throws<InvalidOperationException>(() => schema.Item);
        }

        [Fact]
        public void ThrowsWhenItemSchemaIsSetToNull()
        {
            var schema = new ArraySchema(new IntSchema());
            Assert.Throws<ArgumentNullException>(() => schema.Item = null);
            Assert.IsType<IntSchema>(schema.Item);
        }
    }
}
