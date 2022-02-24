namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="StringSchema" />s
    /// with <see cref="UuidLogicalType" />s.
    /// </summary>
    public class JsonUuidSchemaWriterCase : UuidSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes a <see cref="UnionSchema" /> with a <see cref="UuidLogicalType" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is StringSchema && schema.LogicalType is UuidLogicalType)
            {
                if (canonical)
                {
                    json.WriteStringValue(JsonSchemaToken.String);
                }
                else
                {
                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.String);
                    json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Uuid);
                    json.WriteEndObject();
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonUuidSchemaWriterCase)} can only be applied to {nameof(StringSchema)}s with {nameof(UuidLogicalType)}."));
            }
        }
    }
}
