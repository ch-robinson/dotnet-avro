namespace Chr.Avro.Representation
{
    using System;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="ArraySchema" />s.
    /// </summary>
    public class JsonArraySchemaWriterCase : ArraySchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonArraySchemaWriterCase" /> class.
        /// </summary>
        /// <param name="writer">
        /// A schema writer instance that will be used to use to write item schemas.
        /// </param>
        public JsonArraySchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Gets the schema writer instance that will be used to use to write item schemas.
        /// </summary>
        public IJsonSchemaWriter Writer { get; }

        /// <summary>
        /// Writes an <see cref="ArraySchema" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is ArraySchema arraySchema)
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Array);
                json.WritePropertyName(JsonAttributeToken.Items);
                Writer.Write(arraySchema.Item, json, canonical, context);
                json.WriteEndObject();

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonArraySchemaWriterCase)} can only be applied to {nameof(ArraySchema)}s."));
            }
        }
    }
}
