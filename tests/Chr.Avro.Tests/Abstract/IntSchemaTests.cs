using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class IntSchemaTests
    {
        [Fact]
        public void IsPrimitiveSchema()
        {
            Assert.IsAssignableFrom<PrimitiveSchema>(new IntSchema());
        }
    }
}
