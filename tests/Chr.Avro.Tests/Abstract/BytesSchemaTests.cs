using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class BytesSchemaTests
    {
        [Fact]
        public void IsPrimitiveSchema()
        {
            Assert.IsAssignableFrom<PrimitiveSchema>(new BytesSchema());
        }
    }
}
