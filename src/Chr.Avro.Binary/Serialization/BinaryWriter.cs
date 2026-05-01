namespace Chr.Avro.Serialization
{
    using System;
    using System.Buffers;
#if NET6_0_OR_GREATER
    using System.Buffers.Binary;
#endif
    using System.Text;

    /// <summary>
    /// Writes primitive values to binary Avro data.
    /// </summary>
    public sealed class BinaryWriter
    {
        /// <summary>
        /// The maximum number of characters encoded per chunk when writing strings.
        /// Limiting a chunk to this size keeps the stack-allocated buffer at most
        /// <c>MaxCharChunk * 4</c> bytes (256 bytes for the default value of 64).
        /// </summary>
        internal const int MaxCharChunk = 64;

        private readonly IBufferWriter<byte> output;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryWriter" /> class.
        /// </summary>
        /// <param name="output">
        /// The binary Avro destination.
        /// </param>
        public BinaryWriter(IBufferWriter<byte> output)
        {
            this.output = output;
        }

        /// <summary>
        /// Writes a Boolean value to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="bool" /> value.
        /// </param>
        public void WriteBoolean(bool value)
        {
            var span = output.GetSpan(1);
            span[0] = value ? (byte)0x01 : (byte)0x00;
            output.Advance(1);
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
            var span = output.GetSpan(value.Length);
            new ReadOnlySpan<byte>(value).CopyTo(span);
            output.Advance(value.Length);
        }
#if NET6_0_OR_GREATER

        /// <summary>
        /// Writes fixed-length binary data to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// A span of <see cref="byte" />s.
        /// </param>
        public void WriteFixed(ReadOnlySpan<byte> value)
        {
            var span = output.GetSpan(value.Length);
            value.CopyTo(span);
            output.Advance(value.Length);
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
            var index = 0;

            // Max 5 bytes for 32-bit varint
            Span<byte> buffer = stackalloc byte[5];

            var encoded = (uint)((value << 1) ^ (value >> 31));

            do
            {
                var current = encoded & 0x7FU;
                encoded >>= 7;

                if (encoded != 0)
                {
                    current |= 0x80U;
                }

                buffer[index++] = (byte)current;
            }
            while (encoded != 0U);

            var dest = output.GetSpan(index);
            buffer.Slice(0, index).CopyTo(dest);
            output.Advance(index);
        }

        /// <summary>
        /// Writes a variable-length integer to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="long" /> value.
        /// </param>
        public void WriteInteger(long value)
        {
            var index = 0;

            // Max 10 bytes for 64-bit varint
            Span<byte> buffer = stackalloc byte[10];

            var encoded = (ulong)((value << 1) ^ (value >> 63));

            do
            {
                var current = encoded & 0x7FUL;
                encoded >>= 7;

                if (encoded != 0)
                {
                    current |= 0x80UL;
                }

                buffer[index++] = (byte)current;
            }
            while (encoded != 0UL);

            var dest = output.GetSpan(index);
            buffer.Slice(0, index).CopyTo(dest);
            output.Advance(index);
        }

        /// <summary>
        /// Writes a single-precision floating point number to the current position and advances
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
#if NET6_0_OR_GREATER
            WriteInteger(Encoding.UTF8.GetByteCount(value));

            // UTF8 is Unicode encoding that represents each code point as a sequence of 1 to 4 bytes.
            // By using the max to set the size of our stackalloc'ed buffer, we can be sure we can always
            // encode any UTF8 chars into the buffer without having to perform any checks
            const int MaxBytesPerChar = 4;

            Span<byte> buffer = stackalloc byte[MaxCharChunk * MaxBytesPerChar];

            var pos = 0;
            while (pos < value.Length)
            {
                int remaining = value.Length - pos;
                int sliceLength = remaining < MaxCharChunk ? remaining : MaxCharChunk;
                if (sliceLength < remaining && char.IsHighSurrogate(value[pos + sliceLength - 1]))
                {
                    sliceLength--;
                }

                var chunk = value.AsSpan(pos, sliceLength);
                var written = Encoding.UTF8.GetBytes(chunk, buffer);
                WriteFixed(buffer.Slice(0, written));
                pos += sliceLength;
            }
#else
            WriteBytes(Encoding.UTF8.GetBytes(value));
#endif
        }
    }
}
