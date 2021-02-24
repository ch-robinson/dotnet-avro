namespace Chr.Avro.Representation
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches enum schemas.
    /// </summary>
    public class JsonEnumSchemaReaderCase : EnumSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with a <see cref="BytesSchema" />
        /// or <see cref="FixedSchema" /> if <paramref name="element" /> is a bytes or fixed schema
        /// with a decimal logical type; an unsuccessful <see cref="JsonSchemaReaderCaseResult" />
        /// with an <see cref="UnknownSchemaException" /> otherwise.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a symbols property is not present on the schema.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Enum))
            {
                if (!element.TryGetProperty(JsonAttributeToken.Name, out var name) || name.ValueKind != JsonValueKind.String)
                {
                    throw new InvalidSchemaException($"Named schemas must contain a \"{JsonAttributeToken.Name}\" key.");
                }

                if (!element.TryGetProperty(JsonAttributeToken.Symbols, out var symbols) || symbols.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidSchemaException($"\"{JsonSchemaToken.Enum}\" schemas must contain a \"{JsonAttributeToken.Symbols}\" key.");
                }

                var scope = element.TryGetProperty(JsonAttributeToken.Namespace, out var @namespace)
                    ? @namespace.GetString()
                    : context.Scope;

                var schema = new EnumSchema(QualifyName(name.GetString(), scope))
                {
                    Symbols = symbols.EnumerateArray().Select(symbol => symbol.GetString()).ToArray(),
                };

                if (element.TryGetProperty(JsonAttributeToken.Aliases, out var aliases))
                {
                    schema.Aliases = aliases.EnumerateArray()
                        .Select(alias => QualifyName(alias.GetString(), scope))
                        .ToArray();
                }

                if (element.TryGetProperty(JsonAttributeToken.Doc, out var doc))
                {
                    schema.Documentation = doc.GetString();
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
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonEnumSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Enum}\" schemas."));
            }
        }
    }
}
