using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class BooleanSchemaTests
    {
        [Fact]
        public void IsPrimitiveSchema()
        {
            Assert.IsAssignableFrom<PrimitiveSchema>(new BooleanSchema());
        }
    }
}
