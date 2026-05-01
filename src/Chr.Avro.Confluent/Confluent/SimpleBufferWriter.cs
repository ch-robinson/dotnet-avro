namespace Chr.Avro.Confluent
{
    using System;
    using System.Buffers;

    /// <summary>
    /// A simple <see cref="IBufferWriter{T}" /> implementation backed by a resizable array.
    /// </summary>
    internal sealed class SimpleBufferWriter : IBufferWriter<byte>
    {
        private byte[] buffer;
        private int written;

        public SimpleBufferWriter()
            : this(256)
        {
        }

        public SimpleBufferWriter(int initialCapacity)
        {
            buffer = new byte[initialCapacity];
            written = 0;
        }

        /// <summary>
        /// Gets the number of bytes written so far.
        /// </summary>
        public int WrittenCount => written;

        public void Advance(int count)
        {
            written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint > 0 ? sizeHint : 1);
            return new Memory<byte>(buffer, written, buffer.Length - written);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint > 0 ? sizeHint : 1);
            return new Span<byte>(buffer, written, buffer.Length - written);
        }

        /// <summary>
        /// Returns the written bytes as an array.
        /// </summary>
        public byte[] ToArray()
        {
            var result = new byte[written];
            Array.Copy(buffer, 0, result, 0, written);
            return result;
        }

        /// <summary>
        /// Writes bytes directly to the buffer without going through <see cref="IBufferWriter{T}" />.
        /// Useful for writing a fixed-size header before the main payload.
        /// </summary>
        public void WriteBytes(byte[] data, int offset, int count)
        {
            EnsureCapacity(count);
            Array.Copy(data, offset, buffer, written, count);
            written += count;
        }

        private void EnsureCapacity(int needed)
        {
            if (buffer.Length - written < needed)
            {
                var newSize = Math.Max(buffer.Length * 2, written + needed);
                Array.Resize(ref buffer, newSize);
            }
        }
    }
}
