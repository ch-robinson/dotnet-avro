using System;
using System.Text;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Reads primitive values from binary Avro data.
    /// </summary>
    public ref struct BinaryReader
    {
        private readonly ReadOnlySpan<byte> _data;

        private int _position;

        /// <summary>
        /// Initializes a <see cref="BinaryReader" /> that processes a read-only span of binary
        /// Avro data.
        /// </summary>
        /// <param name="data">
        /// The binary Avro source.
        /// </param>
        public BinaryReader(ReadOnlySpan<byte> data)
        {
            _data = data;
            _position = 0;
        }

        /// <summary>
        /// Reads a boolean from the current position and advances the reader.
        /// </summary>
        public bool ReadBoolean()
        {
            return _data[_position++] == 0 ? false : true;
        }

        /// <summary>
        /// Reads a variable-length array of bytes from the current position and advances the reader.
        /// </summary>
        public byte[] ReadBytes()
        {
            return ReadFixed((int)ReadInteger());
        }

        /// <summary>
        /// Reads a double-precision floating point number from the current position and advances
        /// the reader.
        /// </summary>
        public double ReadDouble()
        {
            var bytes = ReadFixed(8);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToDouble(bytes, 0);
        }

        /// <summary>
        /// Reads a fixed-length array of bytes from the current position and advances the reader.
        /// </summary>
        /// <param name="length">
        /// The number of bytes to read.
        /// </param>
        public byte[] ReadFixed(int length)
        {
            var slice = _data.Slice(_position, length);
            _position += length;

            return slice.ToArray();
        }

        /// <summary>
        /// Reads a variable-length integer from the current position and advances the reader.
        /// </summary>
        public long ReadInteger()
        {
            int current = 0;
            long result = 0;
            byte shift = 0;

            do
            {
                current = _data[_position++];
                result |= (current & 0x7FL) << shift;
                shift += 7;

                if (shift > 70)
                {
                    throw new OverflowException("Invalid Avro long encoding.");
                }
            }
            while (current > 0x7F);

            return (-(result & 0x01)) ^ ((result >> 0x01) & 0x7FFFFFFFFFFFFFFF);
        }

        /// <summary>
        /// Reads a double-precision floating point number from the current position and advances
        /// the reader.
        /// </summary>
        public float ReadSingle()
        {
            var bytes = ReadFixed(4);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToSingle(bytes, 0);
        }

        /// <summary>
        /// Reads a variable-length string from the current position and advances the reader.
        /// </summary>
        public string ReadString()
        {
            return Encoding.UTF8.GetString(ReadBytes());
        }
    }
}
