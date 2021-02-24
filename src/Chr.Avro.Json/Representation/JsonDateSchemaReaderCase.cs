namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches int schemas with date
    /// logical types.
    /// </summary>
    public class JsonDateSchemaReaderCase : DateSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads an <see cref="IntSchema" /> with a <see cref="DateLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="IntSchema" />
        /// if <paramref name="element" /> is an int schema with a date logical type; an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Int)
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueEquals(JsonSchemaToken.Date))
            {
                var key = $"{JsonSchemaToken.Int}!{JsonSchemaToken.Date}";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new IntSchema()
                    {
                        LogicalType = new DateLogicalType(),
                    };

                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonDateSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Int}\" schemas with the \"{JsonSchemaToken.Date}\" logical type."));
            }
        }
    }
}
