namespace Chr.Avro.Representation
{
    using System.IO;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to write serialized Avro schemas.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as the write operation progresses.
    /// </typeparam>
    public interface ISchemaWriter<TContext>
    {
        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// An abstract schema object to write.
        /// </param>
        /// <param name="stream">
        /// A stream to write <paramref name="schema" /> to.
        /// </param>
        /// <param name="canonical">
        /// Whether <paramref name="schema" /> should be written in Parsing Canonical Form (i.e.,
        /// without nonessential attributes).
        /// </param>
        /// <param name="context">
        /// An optional schema writer context. If no context is provided, an empty context will be
        /// created.
        /// </param>
        void Write(Schema schema, Stream stream, bool canonical = false, TContext? context = default);
    }
}
