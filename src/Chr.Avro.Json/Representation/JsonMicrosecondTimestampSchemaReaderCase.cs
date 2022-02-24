namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches long schemas with microsecond
    /// timestamp logical types.
    /// </summary>
    public class JsonMicrosecondTimestampSchemaReaderCase : MicrosecondTimestampSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="LongSchema" /> with a <see cref="MicrosecondTimestampLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="LongSchema" />
        /// if <paramref name="element" /> is a long schema with a microsecond timestamp logical type;
        /// an unsuccessful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Long)
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueEquals(JsonSchemaToken.TimestampMicroseconds))
            {
                var key = $"{JsonSchemaToken.Long}!{JsonSchemaToken.TimestampMicroseconds}";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new LongSchema()
                    {
                        LogicalType = new MicrosecondTimestampLogicalType(),
                    };

                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonMicrosecondTimestampSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Long}\" schemas with the \"{JsonSchemaToken.TimestampMicroseconds}\" logical type."));
            }
        }
    }
}
