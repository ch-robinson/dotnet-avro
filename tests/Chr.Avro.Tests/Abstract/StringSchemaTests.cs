using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class StringSchemaTests
    {
        [Fact]
        public void IsPrimitiveSchema()
        {
            Assert.IsAssignableFrom<PrimitiveSchema>(new StringSchema());
        }
    }
}
