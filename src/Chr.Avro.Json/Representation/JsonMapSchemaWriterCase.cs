namespace Chr.Avro.Representation
{
    using System;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="MapSchema" />s.
    /// </summary>
    public class JsonMapSchemaWriterCase : MapSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonMapSchemaWriterCase" /> class.
        /// </summary>
        /// <param name="writer">
        /// A schema writer instance that will be used to use to write value schemas.
        /// </param>
        public JsonMapSchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Gets the schema writer instance that will be used to use to write value schemas.
        /// </summary>
        public IJsonSchemaWriter Writer { get; }

        /// <summary>
        /// Writes a <see cref="MapSchema" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is MapSchema mapSchema)
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Map);
                json.WritePropertyName(JsonAttributeToken.Values);
                Writer.Write(mapSchema.Value, json, canonical, context);
                json.WriteEndObject();

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonMapSchemaWriterCase)} can only be applied to {nameof(MapSchema)}s."));
            }
        }
    }
}
