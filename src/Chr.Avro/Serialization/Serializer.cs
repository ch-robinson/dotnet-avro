using System.IO;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Creates an Avro representation of an object.
    /// </summary>
    /// <typeparam name="T">
    /// The type of object to serialize.
    /// </typeparam>
    public interface ISerializer<T>
    {
        /// <summary>
        /// Serializes an object.
        /// </summary>
        /// <param name="value">
        /// The object to serialize.
        /// </param>
        /// <param name="stream">
        /// The stream to write the serialized object to.
        /// </param>
        void Serialize(T value, Stream stream);
    }
}
