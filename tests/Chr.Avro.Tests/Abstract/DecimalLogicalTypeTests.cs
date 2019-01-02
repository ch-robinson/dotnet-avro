using Chr.Avro.Abstract;
using System;
using Xunit;

namespace Chr.Avro.Tests
{
    public class DecimalLogicalTypeTests
    {
        [Fact]
        public void IsLogicalType()
        {
            Assert.IsAssignableFrom<LogicalType>(new DecimalLogicalType(1, 0));
        }

        [Fact]
        public void SetsPrecision()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Equal(1, type.Precision);

            type.Precision = 2;
            Assert.Equal(2, type.Precision);
        }

        [Fact]
        public void SetsScale()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Equal(0, type.Scale);

            type.Scale = 1;
            Assert.Equal(1, type.Scale);
        }

        [Fact]
        public void ThrowsWhenPrecisionIsSetToNonPositiveValue()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Precision = 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Precision = -1);
        }

        [Fact]
        public void ThrowsWhenPrecisionIsSetToValueLessThanScale()
        {
            var type = new DecimalLogicalType(8, 4);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Precision = 2);
        }

        [Fact]
        public void ThrowsWhenScaleIsSetToNegativeValue()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Scale = -1);
        }

        [Fact]
        public void ThrowsWhenScaleIsSetToValueGreaterThanPrecision()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Scale = 2);
        }
    }
}
