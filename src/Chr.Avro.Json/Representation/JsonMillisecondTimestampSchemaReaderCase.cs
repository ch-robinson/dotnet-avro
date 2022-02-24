namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches long schemas with millisecond
    /// timestamp logical types.
    /// </summary>
    public class JsonMillisecondTimestampSchemaReaderCase : MillisecondTimestampSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="LongSchema" /> with a <see cref="MillisecondTimestampLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="LongSchema" />
        /// if <paramref name="element" /> is a long schema with a millisecond timestamp logical type;
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
                && logicalType.ValueEquals(JsonSchemaToken.TimestampMilliseconds))
            {
                var key = $"{JsonSchemaToken.Long}!{JsonSchemaToken.TimestampMilliseconds}";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new LongSchema()
                    {
                        LogicalType = new MillisecondTimestampLogicalType(),
                    };

                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonMillisecondTimestampSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Long}\" schemas with the \"{JsonSchemaToken.TimestampMilliseconds}\" logical type."));
            }
        }
    }
}
