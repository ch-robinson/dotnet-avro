namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches int schemas with millisecond
    /// time logical types.
    /// </summary>
    public class JsonMillisecondTimeSchemaReaderCase : MillisecondTimeSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="LongSchema" /> with a <see cref="MillisecondTimeLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="LongSchema" />
        /// if <paramref name="element" /> is an int schema with a millisecond time logical type;
        /// an unsuccessful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Int)
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueEquals(JsonSchemaToken.TimeMilliseconds))
            {
                var key = $"{JsonSchemaToken.Int}!{JsonSchemaToken.TimeMilliseconds}";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new IntSchema()
                    {
                        LogicalType = new MillisecondTimeLogicalType(),
                    };

                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonMillisecondTimeSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Int}\" schemas with the \"{JsonSchemaToken.TimeMilliseconds}\" logical type."));
            }
        }
    }
}
