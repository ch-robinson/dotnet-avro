using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class DoubleSchemaTests
    {
        [Fact]
        public void IsPrimitiveSchema()
        {
            Assert.IsAssignableFrom<PrimitiveSchema>(new DoubleSchema());
        }
    }
}
