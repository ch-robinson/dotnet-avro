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

    /// <summary>
    /// Creates a binary Avro representation of an object.
    /// </summary>
    /// <typeparam name="T">
    /// The type of object to serialize.
    /// </typeparam>
    public class BinarySerializer<T> : IBinarySerializer<T>
    {
        /// <summary>
        /// A serializer delegate.
        /// </summary>
        protected readonly Action<T, Stream> Delegate;

        /// <summary>
        /// Creates a new binary serializer.
        /// </summary>
        /// <param name="delegate">
        /// A serializer delegate.
        /// </param>
        public BinarySerializer(Action<T, Stream> @delegate)
        {
            Delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate), "The encoder implementation cannot be null.");
        }

        /// <summary>
        /// Serializes an object.
        /// </summary>
        /// <param name="value">
        /// The object to serialize.
        /// </param>
        /// <returns>
        /// The binary representation as an array of bytes.
        /// </returns>
        public virtual byte[] Serialize(T value)
        {
            var stream = new MemoryStream();

            using (stream)
            {
                Delegate(value, stream);
            }

            return stream.ToArray();
        }

        /// <summary>
        /// Serializes an object.
        /// </summary>
        /// <param name="value">
        /// The object to serialize.
        /// </param>
        /// <param name="stream">
        /// The stream to write the serialized object to. (The stream will not be disposed.)
        /// </param>
        public virtual void Serialize(T value, Stream stream)
        {
            Delegate(value, stream);
        }
    }
}
