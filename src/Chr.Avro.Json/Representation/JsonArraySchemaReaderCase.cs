namespace Chr.Avro.Representation
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches array schemas.
    /// </summary>
    public class JsonArraySchemaReaderCase : ArraySchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonArraySchemaReaderCase" /> class.
        /// </summary>
        /// <param name="reader">
        /// A schema reader instance that will be used to read item types.
        /// </param>
        public JsonArraySchemaReaderCase(IJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Gets the schema reader instance that will be used to read item types.
        /// </summary>
        public IJsonSchemaReader Reader { get; }

        /// <summary>
        /// Reads an <see cref="ArraySchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="ArraySchema" />
        /// if <paramref name="element" /> is an array schema; an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when an items property is not present on the schema.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Array))
            {
                if (!element.TryGetProperty(JsonAttributeToken.Items, out var items))
                {
                    throw new InvalidSchemaException($"\"{JsonSchemaToken.Array}\" schemas must contain an \"{JsonAttributeToken.Items}\" key.");
                }

                var child = Reader.Read(items, context);
                var key = $"{JsonSchemaToken.Array}<{context.Schemas.Single(p => p.Value == child).Key}>";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new ArraySchema(child);
                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonArraySchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Array}\" schemas."));
            }
        }
    }
}
