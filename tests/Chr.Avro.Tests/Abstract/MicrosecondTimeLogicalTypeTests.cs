using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class MicrosecondTimeLogicalTypeTests
    {
        [Fact]
        public void IsLogicalType()
        {
            Assert.IsAssignableFrom<LogicalType>(new MicrosecondTimeLogicalType());
        }

        [Fact]
        public void IsTimeLogicalType()
        {
            Assert.IsAssignableFrom<TimeLogicalType>(new MicrosecondTimeLogicalType());
        }
    }
}
