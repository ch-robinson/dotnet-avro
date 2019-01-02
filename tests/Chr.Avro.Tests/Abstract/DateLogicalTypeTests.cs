using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class DateLogicalTypeTests
    {
        [Fact]
        public void IsLogicalType()
        {
            Assert.IsAssignableFrom<LogicalType>(new DateLogicalType());
        }
    }
}
