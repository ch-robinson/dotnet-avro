using Chr.Avro.Abstract;
using System.Collections.Concurrent;
using System.IO;

namespace Chr.Avro.Representation
{
    /// <summary>
    /// Writes an Avro schema to a serialized representation.
    /// </summary>
    public interface ISchemaWriter
    {
        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="stream">
        /// The stream to write the schema to.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// An optional schema cache. The cache is populated as the schema is written and can be
        /// used to determine which named schemas have already been processed.
        /// </param>
        void Write(Schema schema, Stream stream, bool canonical = false, ConcurrentDictionary<string, NamedSchema>? names = null);
    }
}
