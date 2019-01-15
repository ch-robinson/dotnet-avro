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

    /// <summary>
    /// Creates an object from a binary Avro representation.
    /// </summary>
    /// <typeparam name="T">
    /// The type of object to deserialize.
    /// </typeparam>
    public class BinaryDeserializer<T> : IBinaryDeserializer<T>
    {
        /// <summary>
        /// A deserializer delegate.
        /// </summary>
        protected readonly Func<Stream, T> Delegate;

        /// <summary>
        /// Creates a new binary deserializer.
        /// </summary>
        /// <param name="delegate">
        /// A deserializer delegate.
        /// </param>
        public BinaryDeserializer(Func<Stream, T> @delegate)
        {
            Delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate), "The decoder implementation cannot be null.");
        }
        
        /// <summary>
        /// Deserializes an object.
        /// </summary>
        /// <param name="blob">
        /// The binary representation as an array of bytes.</param>
        /// <returns>
        /// The deserialized object.
        /// </returns>
        public virtual T Deserialize(byte[] blob)
        {
            using (var stream = new MemoryStream(blob))
            {
                return Delegate(stream);
            }
        }

        /// <summary>
        /// Deserializes an object.
        /// </summary>
        /// <param name="stream">
        /// The stream to read the serialized object from. (The stream will not be disposed.)
        /// </param>
        /// <returns>
        /// The deserialized object.
        /// </returns>
        public virtual T Deserialize(Stream stream)
        {
            return Delegate(stream);
        }
    }
}
