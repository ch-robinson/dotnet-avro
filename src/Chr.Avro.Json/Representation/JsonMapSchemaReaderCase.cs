namespace Chr.Avro.Representation
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches map schemas.
    /// </summary>
    public class JsonMapSchemaReaderCase : MapSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonMapSchemaReaderCase" /> class.
        /// </summary>
        /// <param name="reader">
        /// A schema reader instance that will be used to read value types.
        /// </param>
        public JsonMapSchemaReaderCase(IJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Gets the schema reader instance that will be used to resolve value types.
        /// </summary>
        public IJsonSchemaReader Reader { get; }

        /// <summary>
        /// Reads an <see cref="MapSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="MapSchema" />
        /// if <paramref name="element" /> is an map schema; an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when an values property is not present on the schema.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Map))
            {
                if (!element.TryGetProperty(JsonAttributeToken.Values, out var values))
                {
                    throw new InvalidSchemaException($"\"{JsonSchemaToken.Map}\" schemas must contain an \"{JsonAttributeToken.Values}\" key.");
                }

                var child = Reader.Read(values, context);
                var key = $"{JsonSchemaToken.Map}<{context.Schemas.Single(p => p.Value == child).Key}>";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new MapSchema(child);
                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonMapSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Map}\" schemas."));
            }
        }
    }
}
