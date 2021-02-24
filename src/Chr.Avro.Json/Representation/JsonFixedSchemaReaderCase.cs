namespace Chr.Avro.Representation
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches fixed schemas.
    /// </summary>
    public class JsonFixedSchemaReaderCase : FixedSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="FixedSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with a <see cref="FixedSchema" />
        /// if <paramref name="element" /> is a fixed schema an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when the size property is not present on the schema.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Fixed))
            {
                if (!element.TryGetProperty(JsonAttributeToken.Name, out var name) || name.ValueKind != JsonValueKind.String)
                {
                    throw new InvalidSchemaException($"Named schemas must contain a \"{JsonAttributeToken.Name}\" key.");
                }

                if (!element.TryGetProperty(JsonAttributeToken.Size, out var size) || size.ValueKind != JsonValueKind.Number)
                {
                    throw new InvalidSchemaException($"\"{JsonSchemaToken.Fixed}\" schemas must contain a \"{JsonAttributeToken.Size}\" key.");
                }

                var scope = element.TryGetProperty(JsonAttributeToken.Namespace, out var @namespace)
                    ? @namespace.GetString()
                    : context.Scope;

                var schema = new FixedSchema(QualifyName(name.GetString(), scope), size.GetInt32());

                if (element.TryGetProperty(JsonAttributeToken.Aliases, out var aliases))
                {
                    schema.Aliases = aliases.EnumerateArray()
                        .Select(alias => QualifyName(alias.GetString(), scope))
                        .ToArray();
                }

                try
                {
                    context.Schemas.Add(schema.FullName, schema);
                }
                catch (ArgumentException)
                {
                    throw new InvalidSchemaException($"Invalid name; a definition for {schema.FullName} was already read.");
                }

                foreach (var alias in schema.Aliases)
                {
                    if (alias == schema.FullName)
                    {
                        continue;
                    }

                    try
                    {
                        context.Schemas.Add(alias, schema);
                    }
                    catch (ArgumentException)
                    {
                        throw new InvalidSchemaException($"Invalid alias; a definition for {alias} was already read.");
                    }
                }

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonFixedSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Fixed}\" schemas."));
            }
        }
    }
}
