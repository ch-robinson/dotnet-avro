namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using Xunit;
    using Xunit.Sdk;

    public class BinaryReaderTests
    {
        public static IEnumerable<object[]> BooleanEncodings => new object[][]
        {
            new object[] { false, new byte[] { 0x00 } },
            new object[] { true, new byte[] { 0x01 } },
        };

        public static IEnumerable<object[]> DoubleEncodings => new object[][]
        {
            new object[] { double.NaN, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0xff } },
            new object[] { double.NegativeInfinity, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xff } },
            new object[] { double.MinValue, new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0xff } },
            new object[] { 1.0e-10, new byte[] { 0xbb, 0xbd, 0xd7, 0xd9, 0xdf, 0x7c, 0xdb, 0x3d } },
            new object[] { 0.0, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 } },
            new object[] { 1.0e10, new byte[] { 0x00, 0x00, 0x00, 0x20, 0x5f, 0xa0, 0x02, 0x42 } },
            new object[] { double.MaxValue, new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f } },
            new object[] { double.PositiveInfinity, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x7f } },
        };

        public static IEnumerable<object[]> Int64Encodings => new object[][]
        {
            new object[] { 0L, new byte[] { 0x00 } },
            new object[] { -1L, new byte[] { 0x01 } },
            new object[] { 1L, new byte[] { 0x02 } },
            new object[] { -2L, new byte[] { 0x03 } },
            new object[] { 2L, new byte[] { 0x04 } },
            new object[] { -64L, new byte[] { 0x7f } },
            new object[] { 64L, new byte[] { 0x80, 0x01 } },
            new object[] { -8192L, new byte[] { 0xff, 0x7f } },
            new object[] { 8192L, new byte[] { 0x80, 0x80, 0x01 } },
            new object[] { -4611686018427387904L, new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f } },
            new object[] { 4611686018427387904L, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 } },
            new object[] { long.MinValue, new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 } },
            new object[] { long.MaxValue, new byte[] { 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 } },
        };

        public static IEnumerable<object[]> SingleEncodings => new object[][]
        {
            new object[] { float.NaN, new byte[] { 0x00, 0x00, 0xc0, 0xff } },
            new object[] { float.NegativeInfinity, new byte[] { 0x00, 0x00, 0x80, 0xff } },
            new object[] { float.MinValue, new byte[] { 0xff, 0xff, 0x7f, 0xff } },
            new object[] { 1.0e-10, new byte[] { 0xff, 0xe6, 0xdb, 0x2e } },
            new object[] { 0.0, new byte[] { 0x00, 0x00, 0x00, 0x00 } },
            new object[] { 1.0e10, new byte[] { 0xf9, 0x02, 0x15, 0x50 } },
            new object[] { float.MaxValue, new byte[] { 0xff, 0xff, 0x7f, 0x7f } },
            new object[] { float.PositiveInfinity, new byte[] { 0x00, 0x00, 0x80, 0x7f } },
        };

        public static IEnumerable<object[]> StringEncodings => new object[][]
        {
            new object[] { "test", new byte[] { 0x08, 0x74, 0x65, 0x73, 0x74 } },
            new object[] { "𝄟", new byte[] { 0x08, 0xF0, 0x9D, 0x84, 0x9F } },
        };


        [Theory]
        [MemberData(nameof(BooleanEncodings))]
        [InlineData(true, new byte[] { 0x02 })]
        public void ReadsBooleans(bool value, byte[] encoding)
        {
            var reader = new BinaryReader(encoding);
            Assert.Equal(value, reader.ReadBoolean());
        }

        [Theory]
        [MemberData(nameof(DoubleEncodings))]
        public void ReadsDoubles(double value, byte[] encoding)
        {
            var reader = new BinaryReader(encoding);
            Assert.Equal(value, reader.ReadDouble());
        }

        [Theory]
        [MemberData(nameof(Int64Encodings))]
        public void ReadsIntegers(long value, byte[] encoding)
        {
            var reader = new BinaryReader(encoding);
            Assert.Equal(value, reader.ReadInteger());
        }

        [Theory]
        [MemberData(nameof(SingleEncodings))]
        public void ReadsSingles(float value, byte[] encoding)
        {
            var reader = new BinaryReader(encoding);
            Assert.Equal(value, reader.ReadSingle());
        }

        [Theory]
        [MemberData(nameof(StringEncodings))]
        public void ReadString(string value, byte[] encoding)
        {
            var reader = new BinaryReader(encoding);
            Assert.Equal(value, reader.ReadString());
        }

        [Theory]
        [InlineData(new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 })]
        public void ThrowsOnIntegerOverflow(byte[] encoding)
        {
            var reader = new BinaryReader(encoding);

            try
            {
                var result = reader.ReadInteger();

                // since the reader is a ref struct, can't do Assert.Throws
                throw new ThrowsException(typeof(InvalidEncodingException));
            }
            catch (InvalidEncodingException)
            {
            }
        }
    }
}
