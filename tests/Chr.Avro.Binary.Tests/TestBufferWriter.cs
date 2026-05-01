namespace Chr.Avro.Serialization.Tests
{
    using System;
    using System.Buffers;

    /// <summary>
    /// A simple <see cref="IBufferWriter{T}" /> implementation backed by a resizable array,
    /// used in tests to capture written bytes.
    /// </summary>
    internal sealed class TestBufferWriter : IBufferWriter<byte>
    {
        private byte[] buffer;
        private int written;

        public TestBufferWriter(int initialCapacity = 256)
        {
            buffer = new byte[initialCapacity];
            written = 0;
        }

        public ReadOnlySpan<byte> WrittenSpan => new ReadOnlySpan<byte>(buffer, 0, written);

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
