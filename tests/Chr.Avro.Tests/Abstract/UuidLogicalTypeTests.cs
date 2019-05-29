using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class UuidLogicalTypeTests
    {
        [Fact]
        public void IsLogicalType()
        {
            Assert.IsAssignableFrom<LogicalType>(new UuidLogicalType());
        }
    }
}
