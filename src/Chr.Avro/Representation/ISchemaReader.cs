namespace Chr.Avro.Representation
{
    using System.IO;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to read serialized Avro schemas.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as the read operation progresses.
    /// </typeparam>
    public interface ISchemaReader<TContext>
    {
        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="stream">
        /// A stream to read an encoded schema from.
        /// </param>
        /// <param name="context">
        /// An optional schema reader context. A context can be provided to predefine results for
        /// certain schemas or to grant the caller access to inner results; if no context is
        /// provided, an empty context will be created.
        /// </param>
        /// <returns>
        /// An abstract <see cref="Schema" /> object.
        /// </returns>
        Schema Read(Stream stream, TContext? context = default);
    }
}
