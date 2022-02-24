namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="LongSchema" />s
    /// with <see cref="MicrosecondTimestampLogicalType" />s.
    /// </summary>
    public class JsonMicrosecondTimestampSchemaWriterCase : MicrosecondTimestampSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes a <see cref="LongSchema" /> with a <see cref="MicrosecondTimestampLogicalType" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is LongSchema && schema.LogicalType is MicrosecondTimestampLogicalType)
            {
                if (canonical)
                {
                    json.WriteStringValue(JsonSchemaToken.Long);
                }
                else
                {
                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Long);
                    json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.TimestampMicroseconds);
                    json.WriteEndObject();
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonMicrosecondTimestampSchemaWriterCase)} can only be applied to {nameof(IntSchema)}s with {nameof(DateLogicalType)}."));
            }
        }
    }
}
