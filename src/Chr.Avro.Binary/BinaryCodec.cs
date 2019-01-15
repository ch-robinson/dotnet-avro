using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Handles reading and writing serialized Avro data.
    /// </summary>
    public interface IBinaryCodec
    {
        /// <summary>
        /// Reads bytes from a stream.
        /// </summary>
        byte[] Read(Stream stream, int length);

        /// <summary>
        /// Reads blocks from a stream.
        /// </summary>
        IEnumerable<T> ReadBlocks<T>(Stream stream, Func<Stream, T> @delegate);

        /// <summary>
        /// Reads key-value blocks from a stream.
        /// </summary>
        IDictionary<TKey, TValue> ReadBlocks<TKey, TValue>(Stream stream, Func<Stream, TKey> keyDelegate, Func<Stream, TValue> valueDelegate);

        /// <summary>
        /// Reads a boolean.
        /// </summary>
        bool ReadBoolean(Stream stream);

        /// <summary>
        /// Reads a double-precision floating-point number.
        /// </summary>
        double ReadDouble(Stream stream);

        /// <summary>
        /// Reads a zig-zag encoded integer.
        /// </summary>
        long ReadInteger(Stream stream);

        /// <summary>
        /// Reads a single-precision floating-point number.
        /// </summary>
        float ReadSingle(Stream stream);

        /// <summary>
        /// Writes bytes to a stream.
        /// </summary>
        void Write(byte[] bytes, Stream stream);

        /// <summary>
        /// Writes blocks to a stream.
        /// </summary>
        void WriteBlocks<T>(IEnumerable<T> items, Action<T, Stream> @delegate, Stream stream);

        /// <summary>
        /// Writes key-value blocks to a stream.
        /// </summary>
        void WriteBlocks<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> items, Action<TKey, Stream> keyDelegate, Action<TValue, Stream> valueDelegate, Stream stream);

        /// <summary>
        /// Writes a boolean.
        /// </summary>
        void WriteBoolean(bool value, Stream stream);

        /// <summary>
        /// Writes a double-precision floating-point number.
        /// </summary>
        void WriteDouble(double value, Stream stream);

        /// <summary>
        /// Writes a zig-zag encoded integer.
        /// </summary>
        void WriteInteger(long value, Stream stream);

        /// <summary>
        /// Writes a single-precision floating-point number.
        /// </summary>
        void WriteSingle(float value, Stream stream);
    }

    /// <summary>
    /// A binary codec implementation.
    /// </summary>
    public class BinaryCodec : IBinaryCodec
    {
        /// <summary>
        /// Reads a fixed number of bytes from the stream.
        /// </summary>
        public virtual byte[] Read(Stream stream, int length)
        {
            var bytes = new byte[length];
            stream.Read(bytes, 0, length);

            return bytes;
        }
        
        /// <summary>
        /// Reads blocks from a stream.
        /// </summary>
        public virtual IEnumerable<T> ReadBlocks<T>(Stream stream, Func<Stream, T> @delegate)
        {
            var list = new List<T>();
            long size;

            while ((size = ReadInteger(stream)) != 0L)
            {
                if (size < 0L)
                {
                    size = Math.Abs(size);

                    // negative size indicates that the number of bytes in the block follows, so
                    // discard that:
                    ReadInteger(stream);
                }

                for (var i = 0L; i < size; i++)
                {
                    list.Add(@delegate(stream));
                }
            }

            return list;
        }
        
        /// <summary>
        /// Reads key-value blocks from a stream.
        /// </summary>
        public virtual IDictionary<TKey, TValue> ReadBlocks<TKey, TValue>(Stream stream, Func<Stream, TKey> keyDelegate, Func<Stream, TValue> valueDelegate)
        {
            return ReadBlocks(stream, s => new KeyValuePair<TKey, TValue>(keyDelegate(s), valueDelegate(s)))
                .ToDictionary(p => p.Key, p => p.Value);
        }

        /// <summary>
        /// Reads a boolean.
        /// </summary>
        /// <remarks>
        /// Unlike some implementations, Chr.Avro treats any non-zero byte as true.
        /// </remarks>
        public virtual bool ReadBoolean(Stream stream)
        {
            return stream.ReadByte() != 0x00;
        }

        /// <summary>
        /// Reads a double-precision floating-point number.
        /// </summary>
        public virtual double ReadDouble(Stream stream)
        {
            var bytes = Read(stream, 8);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToDouble(bytes, 0);
        }

        /// <summary>
        /// Reads a zig-zag encoded integer.
        /// </summary>
        /// <exception cref="OverflowException">
        /// Thrown when an encoded integer cannot fit in a <see cref="long" />.
        /// </exception>
        public virtual long ReadInteger(Stream stream)
        {
            byte read = 0;
            int shift = 0;
            ulong result = 0;
            uint chunk;

            do
            {
                read += 1;
                chunk = (uint)stream.ReadByte();
                result |= (chunk & 0x7FUL) << shift;
                shift += 7;

                if (read > 10)
                {
                    throw new OverflowException("Encoded integer exceeds long bounds.");
                }
            }
            while ((chunk & 0x80) != 0);

            var coerced = unchecked((long)result);
            return (-(coerced & 0x1L)) ^ ((coerced >> 1) & 0x7FFFFFFFFFFFFFFFL);
        }

        /// <summary>
        /// Reads a single-precision floating-point number.
        /// </summary>
        public virtual float ReadSingle(Stream stream)
        {
            var bytes = Read(stream, 4);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToSingle(bytes, 0);
        }

        /// <summary>
        /// Writes a boolean.
        /// </summary>
        public virtual void Write(byte[] bytes, Stream stream)
        {
            stream.Write(bytes, 0, bytes.Length);
        }

        /// <summary>
        /// Writes blocks to a stream.
        /// </summary>
        public void WriteBlocks<T>(IEnumerable<T> items, Action<T, Stream> @delegate, Stream stream)
        {
            var list = items.ToList();

            if (list.Count > 0)
            {
                WriteInteger(list.LongCount(), stream);

                foreach (var item in list)
                {
                    @delegate(item, stream);
                }
            }

            WriteInteger(0L, stream);
        }

        /// <summary>
        /// Writes key-value blocks to a stream.
        /// </summary>
        public void WriteBlocks<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> items, Action<TKey, Stream> keyDelegate, Action<TValue, Stream> valueDelegate, Stream stream)
        {
            WriteBlocks(items, (p, s) => { keyDelegate(p.Key, s); valueDelegate(p.Value, s); }, stream);
        }

        /// <summary>
        /// Writes a boolean.
        /// </summary>
        public virtual void WriteBoolean(bool value, Stream stream)
        {
            stream.WriteByte((byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Writes a double-precision floating-point number.
        /// </summary>
        public virtual void WriteDouble(double value, Stream stream)
        {
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            Write(bytes, stream);
        }

        /// <summary>
        /// Writes a zig-zag encoded integer.
        /// </summary>
        public virtual void WriteInteger(long value, Stream stream)
        {
            var encoded = unchecked((ulong)((value << 1) ^ (value >> 63)));

            do
            {
                var chunk = encoded & 0x7f;
                encoded >>= 7;

                if (encoded != 0)
                {
                    chunk |= 0x80;
                }

                stream.WriteByte((byte)chunk);
            }
            while (encoded != 0);
        }

        /// <summary>
        /// Writes a single-precision floating-point number.
        /// </summary>
        public virtual void WriteSingle(float value, Stream stream)
        {
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            Write(bytes, stream);
        }
    }
}
