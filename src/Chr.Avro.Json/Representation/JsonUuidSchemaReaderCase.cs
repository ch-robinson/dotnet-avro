namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches string schemas with UUID
    /// logical types.
    /// </summary>
    public class JsonUuidSchemaReaderCase : UuidSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="StringSchema" /> with a <see cref="UuidLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with a <see cref="StringSchema" />
        /// if <paramref name="element" /> is a string schema with a UUID logical type; an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.String)
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueEquals(JsonSchemaToken.Uuid))
            {
                var key = $"{JsonSchemaToken.String}!{JsonSchemaToken.Uuid}";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new StringSchema()
                    {
                        LogicalType = new UuidLogicalType(),
                    };
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonUuidSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.String}\" schemas with the \"{JsonSchemaToken.Uuid}\" logical type."));
            }
        }
    }
}
