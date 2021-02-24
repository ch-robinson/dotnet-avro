namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="IntSchema" />s
    /// with <see cref="MillisecondTimeLogicalType" />s.
    /// </summary>
    public class JsonMillisecondTimeSchemaWriterCase : MillisecondTimeSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes a <see cref="IntSchema" /> with a <see cref="MillisecondTimeLogicalType" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is IntSchema && schema.LogicalType is MillisecondTimeLogicalType)
            {
                if (canonical)
                {
                    json.WriteStringValue(JsonSchemaToken.Int);
                }
                else
                {
                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Int);
                    json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.TimeMilliseconds);
                    json.WriteEndObject();
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonMillisecondTimeSchemaWriterCase)} can only be applied to {nameof(IntSchema)}s with {nameof(DateLogicalType)}."));
            }
        }
    }
}
