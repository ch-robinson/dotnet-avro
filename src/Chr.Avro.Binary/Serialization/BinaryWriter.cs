namespace Chr.Avro.Serialization
{
    using System;
#if NET6_0_OR_GREATER
    using System.Buffers.Binary;
#endif
    using System.IO;
    using System.Text;

    /// <summary>
    /// Writes primitive values to binary Avro data.
    /// </summary>
    public sealed class BinaryWriter : IDisposable
    {
        private readonly Stream stream;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryWriter" /> class.
        /// </summary>
        /// <param name="stream">
        /// The binary Avro destination.
        /// </param>
        public BinaryWriter(Stream stream)
        {
            this.stream = stream;
        }

        /// <summary>
        /// Frees any resources used by the writer and flushes the <see cref="Stream" />. The
        /// <see cref="Stream" /> is not disposed.
        /// </summary>
        public void Dispose()
        {
            stream.Flush();
        }

        /// <summary>
        /// Writes a Boolean value to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="bool" /> value.
        /// </param>
        public void WriteBoolean(bool value)
        {
            stream.WriteByte(value ? (byte)0x01 : (byte)0x00);
        }

        /// <summary>
        /// Writes variable-length binary data to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// An array of <see cref="byte" />s.
        /// </param>
        public void WriteBytes(byte[] value)
        {
            WriteInteger(value.Length);
            WriteFixed(value);
        }
#if NET6_0_OR_GREATER

        /// <summary>
        /// Writes fixed-length binary data to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// An array of <see cref="byte" />s.
        /// </param>
        public void WriteBytes(ReadOnlySpan<byte> value)
        {
            WriteInteger(value.Length);
            WriteFixed(value);
        }
#endif

        /// <summary>
        /// Writes a double-precision floating-point number to the current position and advances
        /// the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="double" /> value.
        /// </param>
        public void WriteDouble(double value)
        {
#if NET6_0_OR_GREATER
            Span<byte> bytes = stackalloc byte[sizeof(double)];
            BinaryPrimitives.WriteDoubleLittleEndian(bytes, value);
#else
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
#endif

            WriteFixed(bytes);
        }

        /// <summary>
        /// Writes fixed-length binary data to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// An array of <see cref="byte" />s.
        /// </param>
        public void WriteFixed(byte[] value)
        {
            stream.Write(value, 0, value.Length);
        }
#if NET6_0_OR_GREATER

        /// <summary>
        /// Writes fixed-length binary data to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// An array of <see cref="byte" />s.
        /// </param>
        public void WriteFixed(ReadOnlySpan<byte> value)
        {
            stream.Write(value);
        }
#endif

        /// <summary>
        /// Writes a variable-length integer to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// An <see cref="int" /> value.
        /// </param>
        public void WriteInteger(int value)
        {
#if NET6_0_OR_GREATER
            var index = 0;

            // Max 5 bytes for 32-bit varint
            Span<byte> buffer = stackalloc byte[5];
#endif

            var encoded = (uint)((value << 1) ^ (value >> 31));

            do
            {
                var current = encoded & 0x7FU;
                encoded >>= 7;

                if (encoded != 0)
                {
                    current |= 0x80U;
                }

#if NET6_0_OR_GREATER
                buffer[index++] = (byte)current;
#else
                stream.WriteByte((byte)current);
#endif
            }
            while (encoded != 0U);

#if NET6_0_OR_GREATER
            stream.Write(buffer.Slice(0, index));
#endif
        }

        /// <summary>
        /// Writes a variable-length integer to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="long" /> value.
        /// </param>
        public void WriteInteger(long value)
        {
#if NET6_0_OR_GREATER
            var index = 0;

            // Max 10 bytes for 64-bit varint
            Span<byte> buffer = stackalloc byte[10];
#endif

            var encoded = (ulong)((value << 1) ^ (value >> 63));

            do
            {
                var current = encoded & 0x7FUL;
                encoded >>= 7;

                if (encoded != 0)
                {
                    current |= 0x80UL;
                }

#if NET6_0_OR_GREATER
                buffer[index++] = (byte)current;
#else
                stream.WriteByte((byte)current);
#endif
            }
            while (encoded != 0UL);

#if NET6_0_OR_GREATER
            stream.Write(buffer.Slice(0, index));
#endif
        }

        /// <summary>
        /// Writes a double-precision floating point number to the current position and advances
        /// the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="float" /> value.
        /// </param>
        public void WriteSingle(float value)
        {
#if NET6_0_OR_GREATER
            Span<byte> bytes = stackalloc byte[sizeof(float)];
            BinaryPrimitives.WriteSingleLittleEndian(bytes, value);
#else
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
#endif

            WriteFixed(bytes);
        }

        /// <summary>
        /// Writes a UTF-8 string to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="string" /> value.
        /// </param>
        public void WriteString(string value)
        {
            WriteBytes(Encoding.UTF8.GetBytes(value));
        }
    }
}
