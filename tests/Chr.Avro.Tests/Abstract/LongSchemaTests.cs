using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class LongSchemaTests
    {
        [Fact]
        public void IsPrimitiveSchema()
        {
            Assert.IsAssignableFrom<PrimitiveSchema>(new LongSchema());
        }
    }
}
