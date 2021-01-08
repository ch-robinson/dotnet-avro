using System;
using System.IO;
using System.Text;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Writes primitive values to binary Avro data.
    /// </summary>
    public sealed class BinaryWriter : IDisposable
    {
        private readonly Stream _stream;

        /// <summary>
        /// Initializes a <see cref="BinaryWriter" /> that writes binary output to a stream.
        /// </summary>
        /// <param name="stream">
        /// The binary Avro destination.
        /// </param>
        public BinaryWriter(Stream stream)
        {
            _stream = stream;
        }

        /// <summary>
        /// Frees any resources used by the writer and flushes the stream. The stream is not
        /// disposed.
        /// </summary>
        public void Dispose()
        {
            _stream.Flush();
        }

        /// <summary>
        /// Writes a boolean to the current position and advances the writer.
        /// </summary>
        public void WriteBoolean(bool value)
        {
            _stream.WriteByte(value ? (byte)0x01 : (byte)0x00);
        }

        /// <summary>
        /// Writes a variable-length array of bytes to the current position and advances the writer.
        /// </summary>
        public void WriteBytes(byte[] value)
        {
            WriteInteger(value.Length);
            WriteFixed(value);
        }

        /// <summary>
        /// Writes a double-precision floating point number to the current position and advances
        /// the writer.
        /// </summary>
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
        /// Writes a fixed-length array of bytes to the current position and advances the writer.
        /// </summary>
        public void WriteFixed(byte[] value)
        {
            _stream.Write(value, 0, value.Length);
        }

        /// <summary>
        /// Writes a variable-length integer to the current position and advances the writer.
        /// </summary>
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

                _stream.WriteByte((byte)current);
            }
            while (encoded != 0UL);
        }

        /// <summary>
        /// Writes a double-precision floating point number to the current position and advances
        /// the writer.
        /// </summary>
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
        /// Writes a variable-length string to the current position and advances the writer.
        /// </summary>
        public void WriteString(string value)
        {
            WriteBytes(Encoding.UTF8.GetBytes(value));
        }
    }
}
