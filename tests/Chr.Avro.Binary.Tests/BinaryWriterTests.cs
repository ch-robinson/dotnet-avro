namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
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

        public static IEnumerable<object[]> Int32Encodings => new List<object[]>
        {
            new object[] { 0, new byte[] { 0x00 } },
            new object[] { -1, new byte[] { 0x01 } },
            new object[] { 1, new byte[] { 0x02 } },
            new object[] { -2, new byte[] { 0x03 } },
            new object[] { 2, new byte[] { 0x04 } },
            new object[] { -64, new byte[] { 0x7f } },
            new object[] { 64, new byte[] { 0x80, 0x01 } },
            new object[] { -8192, new byte[] { 0xff, 0x7f } },
            new object[] { 8192, new byte[] { 0x80, 0x80, 0x01 } },
            new object[] { int.MinValue, new byte[] { 0xff, 0xff, 0xff, 0xff, 0x0f } },
            new object[] { int.MaxValue, new byte[] { 0xfe, 0xff, 0xff, 0xff, 0x0f } },
        };

        public static IEnumerable<object[]> Int64Encodings => new List<object[]>
        {
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

        public static IEnumerable<object[]> StringEncodings
        {
            get
            {
                return new List<object[]>
                {
                    // String that will be written in a single chunk
                    Create("Hello world"),

                    // Simple string that will require multiple chunks
                    Create(string.Join(", ", Enumerable.Range(0, 1000).Select(i => $"Hello world {i}"))),

                    // String entirely made of char with 2 sequence points that will required multiple chunks
                    Create(string.Join(", ", Enumerable.Range(0, 1000).Select(i => "ç"))),

                    // String entirely made of char with 3 sequence points that will required multiple chunks
                    Create(string.Join(", ", Enumerable.Range(0, 1000).Select(i => "✔"))),

                    // String entirely made of char with 4 sequence points that will required multiple chunks
                    Create(string.Join(", ", Enumerable.Range(0, 1000).Select(i => "𝄞"))),
                };

                static object[] Create(string value)
                {
                    var encoded = EncodeZigZag(Encoding.UTF8.GetByteCount(value)).Concat(Encoding.UTF8.GetBytes(value)).ToArray();
                    return new object[] { value, encoded };
                }

                static byte[] EncodeZigZag(int value)
                {
                    // Max 5 bytes for a 32-bit varint
                    Span<byte> buffer = stackalloc byte[5];
                    var encoded = (uint)((value << 1) ^ (value >> 31));

                    var index = 0;
                    do
                    {
                        var current = encoded & 0x7FU;
                        encoded >>= 7;

                        if (encoded != 0)
                        {
                            current |= 0x80U;
                        }

                        buffer[index] = (byte)current;
                        index++;
                    }
                    while (encoded != 0U);
                    return buffer.Slice(0, index).ToArray();
                }
            }
        }

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
        [MemberData(nameof(Int32Encodings))]
        public void WritesInt32s(int value, byte[] encoding)
        {
            using (stream)
            {
                writer.WriteInteger(value);
            }

            Assert.Equal(encoding, stream.ToArray());
        }

        [Theory]
        [MemberData(nameof(Int32Encodings))]
        [MemberData(nameof(Int64Encodings))]
        public void WritesInt64s(long value, byte[] encoding)
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

        [Theory]
        [MemberData(nameof(StringEncodings))]
        public void WritesStrings(string value, byte[] encoding)
        {
            using (stream)
            {
                writer.WriteString(value);
            }

            Assert.Equal(encoding, stream.ToArray());
        }
    }
}
