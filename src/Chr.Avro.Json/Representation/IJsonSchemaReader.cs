namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to read JSON-serialized Avro schemas.
    /// </summary>
    public interface IJsonSchemaReader : ISchemaReader<JsonSchemaReaderContext>
    {
        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// A JSON-encoded schema.
        /// </param>
        /// <param name="context">
        /// An optional schema reader context. A context can be provided to predefine results for
        /// certain schemas or to grant the caller access to inner results; if no context is
        /// provided, an empty context will be created.
        /// </param>
        /// <returns>
        /// An abstract <see cref="Schema" /> object.
        /// </returns>
        Schema Read(string schema, JsonSchemaReaderContext? context = default);

        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="element">
        /// A JSON element representing a schema.
        /// </param>
        /// <param name="context">
        /// An optional schema reader context. A context can be provided to predefine results for
        /// certain schemas or to grant the caller access to inner results; if no context is
        /// provided, an empty context will be created.
        /// </param>
        /// <returns>
        /// An abstract <see cref="Schema" /> object.
        /// </returns>
        Schema Read(JsonElement element, JsonSchemaReaderContext? context = default);
    }
}
