using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class MillisecondTimeLogicalTypeTests
    {
        [Fact]
        public void IsLogicalType()
        {
            Assert.IsAssignableFrom<LogicalType>(new MillisecondTimeLogicalType());
        }

        [Fact]
        public void IsTimeLogicalType()
        {
            Assert.IsAssignableFrom<TimeLogicalType>(new MillisecondTimeLogicalType());
        }
    }
}
