using Chr.Avro.Abstract;
using System.Collections.Concurrent;
using System.IO;

namespace Chr.Avro.Representation
{
    /// <summary>
    /// Reads an Avro schema from a serialized representation.
    /// </summary>
    public interface ISchemaReader
    {
        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="stream">
        /// The stream to read the serialized schema from.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        /// <returns>
        /// Returns a deserialized schema object.
        /// </returns>
        Schema Read(Stream stream, ConcurrentDictionary<string, Schema> cache = null, string scope = null);
    }
}
