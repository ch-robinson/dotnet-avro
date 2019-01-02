using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class MillisecondTimestampLogicalTypeTests
    {
        [Fact]
        public void IsLogicalType()
        {
            Assert.IsAssignableFrom<LogicalType>(new MillisecondTimestampLogicalType());
        }

        [Fact]
        public void IsTimestampLogicalType()
        {
            Assert.IsAssignableFrom<TimestampLogicalType>(new MillisecondTimestampLogicalType());
        }
    }
}
