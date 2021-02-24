namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches long schemas with microsecond
    /// time logical types.
    /// </summary>
    public class JsonMicrosecondTimeSchemaReaderCase : MicrosecondTimeSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="LongSchema" /> with a <see cref="MicrosecondTimeLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="LongSchema" />
        /// if <paramref name="element" /> is a long schema with a microsecond time logical type;
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
                && logicalType.ValueEquals(JsonSchemaToken.TimeMicroseconds))
            {
                var key = $"{JsonSchemaToken.Long}!{JsonSchemaToken.TimeMicroseconds}";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new LongSchema()
                    {
                        LogicalType = new MicrosecondTimeLogicalType(),
                    };

                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonMicrosecondTimeSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Long}\" schemas with the \"{JsonSchemaToken.TimeMicroseconds}\" logical type."));
            }
        }
    }
}
