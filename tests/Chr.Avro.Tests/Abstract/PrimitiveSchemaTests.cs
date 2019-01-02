using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class PrimitiveSchemaTests
    {
        [Fact]
        public void IsSchema()
        {
            Assert.IsAssignableFrom<Schema>(new ConcretePrimitiveSchema());
        }

        private class ConcretePrimitiveSchema : PrimitiveSchema { }
    }
}
