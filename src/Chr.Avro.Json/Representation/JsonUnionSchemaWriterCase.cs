namespace Chr.Avro.Representation
{
    using System;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaWriter" /> case that matches <see cref="UnionSchema" />s.
    /// </summary>
    public class JsonUnionSchemaWriterCase : UnionSchemaWriterCase, IJsonSchemaWriterCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonUnionSchemaWriterCase" /> class.
        /// </summary>
        /// <param name="writer">
        /// A schema writer instance that will be used to use to write child schemas.
        /// </param>
        public JsonUnionSchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Gets the schema writer instance that will be used to use to write child schemas.
        /// </summary>
        public IJsonSchemaWriter Writer { get; }

        /// <summary>
        /// Writes a <see cref="UnionSchema" />.
        /// </summary>
        /// <inheritdoc />
        public virtual JsonSchemaWriterCaseResult Write(Schema schema, Utf8JsonWriter json, bool canonical, JsonSchemaWriterContext context)
        {
            if (schema is UnionSchema unionSchema)
            {
                json.WriteStartArray();

                foreach (var child in unionSchema.Schemas)
                {
                    Writer.Write(child, json, canonical, context);
                }

                json.WriteEndArray();

                return new JsonSchemaWriterCaseResult();
            }
            else
            {
                return JsonSchemaWriterCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonUnionSchemaWriterCase)} can only be applied to {nameof(UnionSchema)}s."));
            }
        }
    }
}
