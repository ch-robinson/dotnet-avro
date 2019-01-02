using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class FloatSchemaTests
    {
        [Fact]
        public void IsPrimitiveSchema()
        {
            Assert.IsAssignableFrom<PrimitiveSchema>(new FloatSchema());
        }
    }
}
