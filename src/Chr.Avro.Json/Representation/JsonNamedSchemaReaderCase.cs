namespace Chr.Avro.Representation
{
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches references to named schemas.
    /// </summary>
    public class JsonNamedSchemaReaderCase : NamedSchemaReaderCase, IJsonSchemaReaderCase
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
            if (element.ValueKind == JsonValueKind.String)
            {
                var name = element.GetString();
                var qualifiedName = QualifyName(name, context.Scope);

                if (context.Schemas.TryGetValue(qualifiedName, out var schema))
                {
                    return JsonSchemaReaderCaseResult.FromSchema(schema);
                }

                if (name != qualifiedName && context.Schemas.TryGetValue(name, out schema))
                {
                    return JsonSchemaReaderCaseResult.FromSchema(schema);
                }

                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"\"{name}\" is not a known schema."));
            }

            return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonNamedSchemaReaderCase)} can only be applied to named schema references."));
        }
    }
}
