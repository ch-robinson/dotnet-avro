namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="EnumSchema" />s.
    /// </summary>
    public class JsonEnumSchemaWriterCase : EnumSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes an <see cref="EnumSchema" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                if (context.Names.TryGetValue(enumSchema.FullName, out var existing))
                {
                    if (!schema.Equals(existing))
                    {
                        throw new InvalidSchemaException($"A conflicting schema with the name {enumSchema.FullName} has already been written.");
                    }

                    json.WriteStringValue(enumSchema.FullName);
                }
                else
                {
                    context.Names.Add(enumSchema.FullName, enumSchema);

                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Name, enumSchema.FullName);

                    if (!canonical)
                    {
                        if (enumSchema.Aliases.Count > 0)
                        {
                            json.WritePropertyName(JsonAttributeToken.Aliases);
                            json.WriteStartArray();

                            foreach (var alias in enumSchema.Aliases)
                            {
                                json.WriteStringValue(alias);
                            }

                            json.WriteEndArray();
                        }

                        if (!string.IsNullOrEmpty(enumSchema.Documentation))
                        {
                            json.WriteString(JsonAttributeToken.Doc, enumSchema.Documentation);
                        }
                    }

                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Enum);
                    json.WritePropertyName(JsonAttributeToken.Symbols);
                    json.WriteStartArray();

                    foreach (var symbol in enumSchema.Symbols)
                    {
                        json.WriteStringValue(symbol);
                    }

                    json.WriteEndArray();
                    json.WriteEndObject();
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonEnumSchemaWriterCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
