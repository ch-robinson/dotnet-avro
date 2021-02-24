namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to write specific Avro schemas to JSON.
    /// </summary>
    /// <inheritdoc />
    public interface IJsonSchemaWriterCase : ISchemaWriterCase<JsonSchemaWriterContext, JsonSchemaWriterCaseResult>
    {
        /// <summary>
        /// Writes a schema to JSON.
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
        /// A <see cref="JsonSchemaWriterContext" /> representing the state of the write operation.
        /// </param>
        /// <returns>
        /// A successful <see cref="JsonSchemaWriterCaseResult" /> if the case can be applied; an
        /// unsuccessful <see cref="JsonSchemaWriterCaseResult" /> otherwise.
        /// </returns>
        JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context);
    }
}
