namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Xunit;

    using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

    public class BinaryWriterTests
    {
        private readonly MemoryStream stream;

        private readonly BinaryWriter writer;

        public BinaryWriterTests()
        {
            stream = new MemoryStream();
            writer = new BinaryWriter(stream);
        }

        public static IEnumerable<object[]> BlockEncodings => new List<object[]>
        {
            new object[] { Array.Empty<byte>(), new byte[] { 0x00 } },
            new object[] { new byte[] { 0x00, 0x01, 0x02 }, new byte[] { 0x06, 0x00, 0x01, 0x02, 0x00 } },
        };

        public static IEnumerable<object[]> BlockEncodingsWithMultipleBlocks => new List<object[]>
        {
            new object[] { new byte[] { 0x00, 0x01, 0x02 }, new byte[] { 0x02, 0x00, 0x04, 0x01, 0x02, 0x00 } },
        };

        public static IEnumerable<object[]> BlockEncodingsWithNegativeSize => new List<object[]>
        {
            new object[] { new byte[] { 0x02 }, new byte[] { 0x01, 0x01, 0x02, 0x00 } },
            new object[] { new byte[] { 0x00, 0x01, 0x02 }, new byte[] { 0x05, 0x03, 0x00, 0x01, 0x02, 0x00 } },
        };

        public static IEnumerable<object[]> BooleanEncodings => new List<object[]>
        {
            new object[] { false, new byte[] { 0x00 } },
            new object[] { true, new byte[] { 0x01 } },
        };

        public static IEnumerable<object[]> DoubleEncodings => new List<object[]>
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

        public static IEnumerable<object[]> IntegerEncodings => new List<object[]>
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

        public static IEnumerable<object[]> SingleEncodings => new List<object[]>
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

        [Theory]
        [MemberData(nameof(BooleanEncodings))]
        public void WritesBooleans(bool value, byte[] encoding)
        {
            using (stream)
            {
                writer.WriteBoolean(value);
            }

            Assert.Equal(encoding, stream.ToArray());
        }

        [Theory]
        [MemberData(nameof(DoubleEncodings))]
        public void WritesDoubles(double value, byte[] encoding)
        {
            using (stream)
            {
                writer.WriteDouble(value);
            }

            Assert.Equal(encoding, stream.ToArray());
        }

        [Theory]
        [MemberData(nameof(IntegerEncodings))]
        public void WritesIntegers(long value, byte[] encoding)
        {
            using (stream)
            {
                writer.WriteInteger(value);
            }

            Assert.Equal(encoding, stream.ToArray());
        }

        [Theory]
        [MemberData(nameof(SingleEncodings))]
        public void WritesSingles(float value, byte[] encoding)
        {
            using (stream)
            {
                writer.WriteSingle(value);
            }

            Assert.Equal(encoding, stream.ToArray());
        }
    }
}
