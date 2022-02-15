namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="BytesSchema" />s
    /// or <see cref="FixedSchema" />s with <see cref="DecimalLogicalType" />.
    /// </summary>
    public class JsonDecimalSchemaWriterCase : DecimalSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Writes a <see cref="BytesSchema" /> or <see cref="FixedSchema" /> with a
        /// <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                if (schema is FixedSchema fixedSchema)
                {
                    if (context.Names.TryGetValue(fixedSchema.FullName, out var existing))
                    {
                        if (!schema.Equals(existing))
                        {
                            throw new InvalidSchemaException($"A conflicting schema with the name {fixedSchema.FullName} has already been written.");
                        }

                        json.WriteStringValue(fixedSchema.FullName);
                    }
                    else
                    {
                        context.Names.Add(fixedSchema.FullName, fixedSchema);

                        json.WriteStartObject();
                        json.WriteString(JsonAttributeToken.Name, fixedSchema.FullName);

                        if (!canonical)
                        {
                            if (fixedSchema.Aliases.Count > 0)
                            {
                                json.WritePropertyName(JsonAttributeToken.Aliases);
                                json.WriteStartArray();

                                foreach (var alias in fixedSchema.Aliases)
                                {
                                    json.WriteStringValue(alias);
                                }

                                json.WriteEndArray();
                            }
                        }

                        json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Fixed);

                        if (!canonical)
                        {
                            json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Decimal);
                            json.WriteNumber(JsonAttributeToken.Precision, decimalLogicalType.Precision);
                            json.WriteNumber(JsonAttributeToken.Scale, decimalLogicalType.Scale);
                        }

                        json.WriteNumber(JsonAttributeToken.Size, fixedSchema.Size);
                        json.WriteEndObject();
                    }
                }
                else if (schema is BytesSchema)
                {
                    if (canonical)
                    {
                        json.WriteStringValue(JsonSchemaToken.Bytes);
                    }
                    else
                    {
                        json.WriteStartObject();
                        json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Bytes);
                        json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Decimal);
                        json.WriteNumber(JsonAttributeToken.Precision, decimalLogicalType.Precision);
                        json.WriteNumber(JsonAttributeToken.Scale, decimalLogicalType.Scale);
                        json.WriteEndObject();
                    }
                }
                else
                {
                    throw new UnsupportedSchemaException(schema, $"A {nameof(DecimalLogicalType)} can only be written for a {nameof(BytesSchema)} or {nameof(FixedSchema)}.");
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDecimalSchemaWriterCase)} can only be applied to {nameof(BytesSchema)}s or {nameof(FloatSchema)}s with {nameof(DecimalLogicalType)}."));
            }
        }
    }
}
