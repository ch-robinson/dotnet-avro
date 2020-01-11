using System;
using System.IO;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Creates a binary Avro representation of an object.
    /// </summary>
    /// <typeparam name="T">
    /// The type of object to serialize.
    /// </typeparam>
    public interface IBinarySerializer<T> : ISerializer<T>
    {
        /// <summary>
        /// Serializes an object.
        /// </summary>
        /// <param name="value">
        /// The object to serialize.
        /// </param>
        /// <returns>
        /// The binary representation as an array of bytes.
        /// </returns>
        byte[] Serialize(T value);
    }

    internal class BinarySerializer<T> : IBinarySerializer<T>
    {
        protected readonly Action<T, Stream> Delegate;

        public BinarySerializer(Action<T, Stream> @delegate)
        {
            Delegate = @delegate;
        }

        public virtual byte[] Serialize(T value)
        {
            var stream = new MemoryStream();

            using (stream)
            {
                Delegate(value, stream);
            }

            return stream.ToArray();
        }

        public virtual void Serialize(T value, Stream stream)
        {
            Delegate(value, stream);
        }
    }
}
