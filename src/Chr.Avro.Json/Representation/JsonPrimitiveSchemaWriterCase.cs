namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="PrimitiveSchema" />s.
    /// </summary>
    public class JsonPrimitiveSchemaWriterCase : PrimitiveSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes an <see cref="PrimitiveSchema" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is PrimitiveSchema primitiveSchema)
            {
                json.WriteStringValue(primitiveSchema switch
                {
                    BooleanSchema _ => JsonSchemaToken.Boolean,
                    BytesSchema _ => JsonSchemaToken.Bytes,
                    DoubleSchema _ => JsonSchemaToken.Double,
                    FloatSchema _ => JsonSchemaToken.Float,
                    IntSchema _ => JsonSchemaToken.Int,
                    LongSchema _ => JsonSchemaToken.Long,
                    NullSchema _ => JsonSchemaToken.Null,
                    StringSchema _ => JsonSchemaToken.String,
                    _ => throw new UnsupportedSchemaException(schema, $"Unknown primitive schema {schema}."),
                });

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonPrimitiveSchemaWriterCase)} can only be applied to {nameof(PrimitiveSchema)}s."));
            }
        }
    }
}
