using System;
using System.IO;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Creates an object from a binary Avro representation.
    /// </summary>
    /// <typeparam name="T">
    /// The type of object to deserialize.
    /// </typeparam>
    public interface IBinaryDeserializer<T> : IDeserializer<T>
    {
        /// <summary>
        /// Deserializes an object.
        /// </summary>
        /// <param name="blob">
        /// The binary representation as an array of bytes.</param>
        /// <returns>
        /// The deserialized object.
        /// </returns>
        T Deserialize(byte[] blob);
    }

    internal class BinaryDeserializer<T> : IBinaryDeserializer<T>
    {
        protected readonly Func<Stream, T> Delegate;

        public BinaryDeserializer(Func<Stream, T> @delegate)
        {
            Delegate = @delegate;
        }

        public virtual T Deserialize(byte[] blob)
        {
            using (var stream = new MemoryStream(blob))
            {
                return Delegate(stream);
            }
        }

        public virtual T Deserialize(Stream stream)
        {
            return Delegate(stream);
        }
    }
}
