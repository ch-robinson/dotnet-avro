namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="LongSchema" />s
    /// with <see cref="MillisecondTimestampLogicalType" />s.
    /// </summary>
    public class JsonMillisecondTimestampSchemaWriterCase : MillisecondTimestampSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes a <see cref="LongSchema" /> with a <see cref="MillisecondTimestampLogicalType" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is LongSchema && schema.LogicalType is MillisecondTimestampLogicalType)
            {
                if (canonical)
                {
                    json.WriteStringValue(JsonSchemaToken.Long);
                }
                else
                {
                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Long);
                    json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.TimestampMilliseconds);
                    json.WriteEndObject();
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonMillisecondTimestampSchemaWriterCase)} can only be applied to {nameof(IntSchema)}s with {nameof(DateLogicalType)}."));
            }
        }
    }
}
