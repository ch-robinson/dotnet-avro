using System.IO;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Creates an object from an Avro representation.
    /// </summary>
    /// <typeparam name="T">
    /// The type of object to deserialize.
    /// </typeparam>
    public interface IDeserializer<T>
    {
        /// <summary>
        /// Deserializes an object.
        /// </summary>
        /// <param name="stream">
        /// The stream to read the serialized object from.
        /// </param>
        /// <returns>
        /// The deserialized object.
        /// </returns>
        T Deserialize(Stream stream);
    }
}
