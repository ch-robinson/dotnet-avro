namespace Chr.Avro.Serialization
{
    using System;
    using System.Text;

    /// <summary>
    /// Reads primitive values from binary Avro data.
    /// </summary>
    public ref struct BinaryReader
    {
        private readonly ReadOnlySpan<byte> data;

        private int index;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryReader" /> struct.
        /// </summary>
        /// <param name="data">
        /// The binary Avro source.
        /// </param>
        public BinaryReader(ReadOnlySpan<byte> data)
        {
            this.data = data;
            this.index = 0;
        }

        /// <summary>
        /// Gets the current position of the reader.
        /// </summary>
        public long Index => index;

        /// <summary>
        /// Reads a Boolean value from the current position and advances the reader.
        /// </summary>
        /// <returns>
        /// <c>false</c> if the byte at the current position is <c>0</c>; <c>true</c> otherwise.
        /// </returns>
        public bool ReadBoolean()
        {
            return data[index++] != 0;
        }

        /// <summary>
        /// Reads variable-length binary data from the current position and advances the reader.
        /// </summary>
        /// <returns>
        /// An array of <see cref="byte" />s with length specified by the integer at the current
        /// position.
        /// </returns>
        public byte[] ReadBytes()
        {
            return ReadFixed((int)ReadInteger());
        }

        /// <summary>
        /// Reads variable-length binary data from the current position and advances the reader.
        /// </summary>
        /// <returns>
        /// An span of <see cref="byte" />s with length specified by the integer at the current
        /// position.
        /// </returns>
        public ReadOnlySpan<byte> ReadSpan()
        {
            return ReadFixedSpan((int)ReadInteger());
        }

        /// <summary>
        /// Reads a double-precision floating-point number from the current position and advances
        /// the reader.
        /// </summary>
        /// <returns>
        /// The bytes at the current position as a <see cref="double" /> (big-endian).
        /// </returns>
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
        /// Reads fixed-length binary data from the current position and advances the reader.
        /// </summary>
        /// <param name="length">
        /// The number of bytes to read.
        /// </param>
        /// <returns>
        /// An array of <see cref="byte" />s with length specified by <paramref name="length" />.
        /// </returns>
        public byte[] ReadFixed(int length)
        {
            return ReadFixedSpan(length).ToArray();
        }

        /// <summary>
        /// Reads fixed-length binary data from the current position and advances the reader.
        /// </summary>
        /// <param name="length">
        /// The number of bytes to read.
        /// </param>
        /// <returns>
        /// An span of <see cref="byte" />s with length specified by <paramref name="length" />.
        /// </returns>
        public ReadOnlySpan<byte> ReadFixedSpan(int length)
        {
            var slice = data.Slice(index, length);
            index += length;
            return slice;
        }

        /// <summary>
        /// Reads a variable-length integer from the current position and advances the reader.
        /// </summary>
        /// <returns>
        /// The bytes at the current position as a <see cref="long" />.
        /// </returns>
        public long ReadInteger()
        {
            int current;
            long result = 0;
            byte shift = 0;

            do
            {
                current = data[index++];
                result |= (current & 0x7FL) << shift;
                shift += 7;

                if (shift > 70)
                {
                    throw new InvalidEncodingException(index, "Unable to read a valid variable-length integer. This may indicate invalid encoding earlier in the stream.");
                }
            }
            while (current > 0x7F);

            return (-(result & 0x01)) ^ ((result >> 0x01) & 0x7FFFFFFFFFFFFFFF);
        }

        /// <summary>
        /// Reads a single-precision floating-point number from the current position and advances
        /// the reader.
        /// </summary>
        /// <returns>
        /// The bytes at the current position as a <see cref="float" /> (big-endian).
        /// </returns>
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
        /// Reads a UTF-8 string from the current position and advances the reader.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> with a byte length specified by the integer at the current
        /// position.
        /// </returns>
        public string ReadString()
        {
            return Encoding.UTF8.GetString(ReadBytes());
        }
    }
}
