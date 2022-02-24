namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="IntSchema" />s
    /// with <see cref="DateLogicalType" />.
    /// </summary>
    public class JsonDateSchemaWriterCase : DateSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes an <see cref="IntSchema" /> with a <see cref="DateLogicalType" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is IntSchema && schema.LogicalType is DateLogicalType)
            {
                if (canonical)
                {
                    json.WriteStringValue(JsonSchemaToken.Int);
                }
                else
                {
                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Int);
                    json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Date);
                    json.WriteEndObject();
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDateSchemaWriterCase)} can only be applied to {nameof(IntSchema)}s with {nameof(DateLogicalType)}."));
            }
        }
    }
}
