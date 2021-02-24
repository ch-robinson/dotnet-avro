namespace Chr.Avro.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Xunit;

    public class DecimalLogicalTypeShould
    {
        [Fact]
        public void SetPrecision()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Equal(1, type.Precision);

            type.Precision = 2;
            Assert.Equal(2, type.Precision);
        }

        [Fact]
        public void SetScale()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Equal(0, type.Scale);

            type.Scale = 1;
            Assert.Equal(1, type.Scale);
        }

        [Fact]
        public void ThrowWhenPrecisionIsSetToNonPositiveValue()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Precision = 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Precision = -1);
        }

        [Fact]
        public void ThrowWhenPrecisionIsSetToValueLessThanScale()
        {
            var type = new DecimalLogicalType(8, 4);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Precision = 2);
        }

        [Fact]
        public void ThrowWhenScaleIsSetToNegativeValue()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Scale = -1);
        }

        [Fact]
        public void ThrowWhenScaleIsSetToValueGreaterThanPrecision()
        {
            var type = new DecimalLogicalType(1, 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => type.Scale = 2);
        }
    }
}
