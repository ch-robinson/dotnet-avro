namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to write JSON-serialized Avro schemas.
    /// </summary>
    public interface IJsonSchemaWriter : ISchemaWriter<JsonSchemaWriterContext>
    {
        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// An abstract schema object to write.
        /// </param>
        /// <param name="canonical">
        /// Whether <paramref name="schema" /> should be written in Parsing Canonical Form (i.e.,
        /// without nonessential attributes).
        /// </param>
        /// <param name="context">
        /// An optional schema writer context. If no context is provided, an empty context will be
        /// created.
        /// </param>
        /// <returns>
        /// A JSON-encoded schema.
        /// </returns>
        string Write(Schema schema, bool canonical = false, JsonSchemaWriterContext? context = default);

        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// An abstract schema object to write.
        /// </param>
        /// <param name="json">
        /// A writer to use for JSON operations.
        /// </param>
        /// <param name="canonical">
        /// Whether <paramref name="schema" /> should be written in Parsing Canonical Form (i.e.,
        /// without nonessential attributes).
        /// </param>
        /// <param name="context">
        /// An optional schema writer context. If no context is provided, an empty context will be
        /// created.
        /// </param>
        void Write(Schema schema, Utf8JsonWriter json, bool canonical = false, JsonSchemaWriterContext? context = default);
    }
}
