namespace Chr.Avro.Abstract.Tests
{
    using System;
    using System.Text.Json;
    using Xunit;

    public class ObjectDefaultValueShould
    {
        [Fact]
        public void ConvertValueToObject()
        {
            var schema = new IntSchema();

            var defaultValue = new ObjectDefaultValue<int?>(5, schema);
            Assert.Equal(5, defaultValue.Value);
            Assert.Equal(5, defaultValue.ToObject<int>());

            defaultValue.Value = null;
            Assert.Null(defaultValue.Value);
            Assert.Throws<UnsupportedTypeException>(() => defaultValue.ToObject<int>());
        }

        [Fact]
        public void ThrowWhenConstructedWithNullSchema()
        {
            Assert.Throws<ArgumentNullException>(() => new ObjectDefaultValue<int>(5, null));
        }

        [Fact]
        public void UseFirstChildOfUnionSchema()
        {
            var @int = new IntSchema();
            var @string = new StringSchema();
            var union = new UnionSchema(new Schema[] { @int, @string });

            var defaultValue = new ObjectDefaultValue<int>(1, union);
            Assert.Equal(@int, defaultValue.Schema);
        }
    }
}
