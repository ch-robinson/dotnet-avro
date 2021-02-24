namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches primitive schemas.
    /// </summary>
    public class JsonPrimitiveSchemaReaderCase : PrimitiveSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="PrimitiveSchema " />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with a <see cref="PrimitiveSchema" />
        /// if <paramref name="element" /> is a primitive schema; an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                element.TryGetProperty(JsonAttributeToken.Type, out element);
            }

            if (element.ValueKind == JsonValueKind.String)
            {
                var key = element.GetString();

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = key switch
                    {
                        JsonSchemaToken.Boolean => new BooleanSchema(),
                        JsonSchemaToken.Bytes => new BytesSchema(),
                        JsonSchemaToken.Double => new DoubleSchema(),
                        JsonSchemaToken.Float => new FloatSchema(),
                        JsonSchemaToken.Int => new IntSchema(),
                        JsonSchemaToken.Long => new LongSchema(),
                        JsonSchemaToken.Null => new NullSchema(),
                        JsonSchemaToken.String => new StringSchema(),
                        _ => null
                    };

                    if (schema != null)
                    {
                        context.Schemas.Add(key, schema);
                    }
                }

                if (schema != null)
                {
                    return JsonSchemaReaderCaseResult.FromSchema(schema);
                }
            }

            return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonPrimitiveSchemaReaderCase)} can only be applied to valid primitive schemas."));
        }
    }
}
