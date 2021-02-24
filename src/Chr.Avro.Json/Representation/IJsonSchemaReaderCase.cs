namespace Chr.Avro.Representation
{
    using System.Text.Json;

    /// <summary>
    /// Defines methods to read Avro schemas from specific JSON elements.
    /// </summary>
    /// <inheritdoc />
    public interface IJsonSchemaReaderCase : ISchemaReaderCase<JsonSchemaReaderContext, JsonSchemaReaderCaseResult>
    {
        /// <summary>
        /// Reads a schema from a JSON element.
        /// </summary>
        /// <param name="element">
        /// An element to parse.
        /// </param>
        /// <param name="context">
        /// A <see cref="JsonSchemaReaderContext" /> representing the state of the build operation.
        /// </param>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> if the case can be applied; an
        /// unsuccessful <see cref="JsonSchemaReaderCaseResult" /> otherwise.
        /// </returns>
        JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context);
    }
}
