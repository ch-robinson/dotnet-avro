namespace Chr.Avro.Serialization
{
    using System;
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

        /// <summary>
        /// Writes a double-precision floating-point number to the current position and advances
        /// the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="double" /> value.
        /// </param>
        public void WriteDouble(double value)
        {
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

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

        /// <summary>
        /// Writes a variable-length integer to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// An <see cref="int" /> value.
        /// </param>
        public void WriteInteger(int value)
        {
            var encoded = (uint)((value << 1) ^ (value >> 31));

            do
            {
                var current = encoded & 0x7FU;
                encoded >>= 7;

                if (encoded != 0)
                {
                    current |= 0x80U;
                }

                stream.WriteByte((byte)current);
            }
            while (encoded != 0U);
        }

        /// <summary>
        /// Writes a variable-length integer to the current position and advances the writer.
        /// </summary>
        /// <param name="value">
        /// A <see cref="long" /> value.
        /// </param>
        public void WriteInteger(long value)
        {
            var encoded = (ulong)((value << 1) ^ (value >> 63));

            do
            {
                var current = encoded & 0x7FUL;
                encoded >>= 7;

                if (encoded != 0)
                {
                    current |= 0x80UL;
                }

                stream.WriteByte((byte)current);
            }
            while (encoded != 0UL);
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
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

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
