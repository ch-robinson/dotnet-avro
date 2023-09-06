namespace Chr.Avro.Abstract.Tests
{
    using System;
    using System.Text.Json;
    using Xunit;

    public class JsonDefaultValueShould
    {
        [Fact]
        public void ConvertValueToObject()
        {
            using var document = JsonDocument.Parse("1");
            var element = document.RootElement;

            var schema = new IntSchema();

            var defaultValue = new JsonDefaultValue(element, schema);
            Assert.Equal(JsonValueKind.Number, defaultValue.Element.ValueKind);
            Assert.Equal(1, defaultValue.ToObject<int>());
        }

        [Fact]
        public void ThrowWhenConstructedWithNullSchema()
        {
            using var document = JsonDocument.Parse("1");
            var element = document.RootElement;

            Assert.Throws<ArgumentNullException>(() => new JsonDefaultValue(element, null));
        }

        [Fact]
        public void UseFirstChildOfUnionSchema()
        {
            using var document = JsonDocument.Parse("1");
            var element = document.RootElement;

            var @int = new IntSchema();
            var @string = new StringSchema();
            var union = new UnionSchema(new Schema[] { @int, @string });

            var defaultValue = new JsonDefaultValue(element, union);
            Assert.Equal(@int, defaultValue.Schema);
        }
    }
}
