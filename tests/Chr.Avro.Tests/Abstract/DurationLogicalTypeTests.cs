using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class DurationLogicalTypeTests
    {
        [Fact]
        public void IsLogicalType()
        {
            Assert.IsAssignableFrom<LogicalType>(new DurationLogicalType());
        }
    }
}
