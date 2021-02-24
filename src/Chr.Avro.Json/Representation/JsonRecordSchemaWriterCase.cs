namespace Chr.Avro.Representation
{
    using System;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="RecordSchema" />s.
    /// </summary>
    public class JsonRecordSchemaWriterCase : RecordSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonRecordSchemaWriterCase" /> class.
        /// </summary>
        /// <param name="writer">
        /// A schema writer instance that will be used to use to write field schemas.
        /// </param>
        public JsonRecordSchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Gets the schema writer instance that will be used to use to write field schemas.
        /// </summary>
        public IJsonSchemaWriter Writer { get; }

        /// <summary>
        /// Writes a <see cref="RecordSchema" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                if (context.Names.TryGetValue(recordSchema.FullName, out var existing))
                {
                    if (!schema.Equals(existing))
                    {
                        throw new InvalidSchemaException($"A conflicting schema with the name {recordSchema.FullName} has already been written.");
                    }

                    json.WriteStringValue(recordSchema.FullName);
                }
                else
                {
                    context.Names.Add(recordSchema.FullName, recordSchema);

                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Name, recordSchema.FullName);

                    if (!canonical)
                    {
                        if (recordSchema.Aliases.Count > 0)
                        {
                            json.WritePropertyName(JsonAttributeToken.Aliases);
                            json.WriteStartArray();

                            foreach (var alias in recordSchema.Aliases)
                            {
                                json.WriteStringValue(alias);
                            }

                            json.WriteEndArray();
                        }

                        if (!string.IsNullOrEmpty(recordSchema.Documentation))
                        {
                            json.WriteString(JsonAttributeToken.Doc, recordSchema.Documentation);
                        }
                    }

                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Record);
                    json.WritePropertyName(JsonAttributeToken.Fields);
                    json.WriteStartArray();

                    foreach (var field in recordSchema.Fields)
                    {
                        json.WriteStartObject();
                        json.WriteString(JsonAttributeToken.Name, field.Name);

                        if (!canonical && !string.IsNullOrEmpty(field.Documentation))
                        {
                            json.WriteString(JsonAttributeToken.Doc, field.Documentation);
                        }

                        json.WritePropertyName(JsonAttributeToken.Type);
                        Writer.Write(field.Type, json, canonical, context);
                        json.WriteEndObject();
                    }

                    json.WriteEndArray();
                    json.WriteEndObject();
                }

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonRecordSchemaWriterCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
        }
    }
}
