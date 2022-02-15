namespace Chr.Avro.Representation
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches union schemas.
    /// </summary>
    public class JsonUnionSchemaReaderCase : UnionSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonUnionSchemaReaderCase" /> class.
        /// </summary>
        /// <param name="reader">
        /// A schema reader instance that will be used to read child types.
        /// </param>
        public JsonUnionSchemaReaderCase(IJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Gets the schema reader instance that will be used to read child types.
        /// </summary>
        public IJsonSchemaReader Reader { get; }

        /// <summary>
        /// Reads a <see cref="UnionSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with a <see cref="UnionSchema" />
        /// if <paramref name="element" /> is a union schema; an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Array)
            {
                var children = element
                    .EnumerateArray()
                    .Select(child => Reader.Read(child, context))
                    .ToArray();

                var key = $"[{string.Join(",", children.Select(s => context.Schemas.Single(p => p.Value == s).Key))}]";

                if (!context.Schemas.TryGetValue(key, out var schema))
                {
                    schema = new UnionSchema(children);
                    context.Schemas.Add(key, schema);
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonUnionSchemaReaderCase)} can only be applied to union schemas."));
            }
        }
    }
}
