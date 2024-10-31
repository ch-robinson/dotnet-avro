namespace Chr.Avro.Representation
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches bytes or fixed schemas with
    /// decimal logical types.
    /// </summary>
    public class JsonDecimalSchemaReaderCase : DecimalSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Reads a <see cref="BytesSchema" /> or <see cref="FixedSchema" /> with a
        /// <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with a <see cref="BytesSchema" />
        /// or <see cref="FixedSchema" /> if <paramref name="element" /> is a bytes or fixed schema
        /// with a decimal logical type; an unsuccessful <see cref="JsonSchemaReaderCaseResult" />
        /// with an <see cref="UnknownSchemaException" /> otherwise.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when precision or scale properties are not present on the schema.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueEquals(JsonSchemaToken.Decimal))
            {
                if (!element.TryGetProperty(JsonAttributeToken.Precision, out var precision) || precision.ValueKind != JsonValueKind.Number)
                {
                    throw new InvalidSchemaException($"Schemas with the \"{JsonSchemaToken.Decimal}\" logical type must contain a \"{JsonAttributeToken.Precision}\" key.");
                }

                if (element.TryGetProperty(JsonAttributeToken.Scale, out var scale) && scale.ValueKind != JsonValueKind.Number)
                {
                    throw new InvalidSchemaException($"Schemas with the \"{JsonSchemaToken.Decimal}\" logical type must contain a \"{JsonAttributeToken.Scale}\" key.");
                }

                if (type.ValueEquals(JsonSchemaToken.Bytes))
                {
                    var key = $"{JsonSchemaToken.Bytes}!{JsonSchemaToken.Decimal}!{precision.GetInt32()}!{(scale.ValueKind == JsonValueKind.Undefined ? 0 : scale.GetInt32())}";

                    if (!context.Schemas.TryGetValue(key, out var schema))
                    {
                        schema = new BytesSchema()
                        {
                            LogicalType = new DecimalLogicalType(precision.GetInt32(), scale.ValueKind == JsonValueKind.Undefined ? 0 : scale.GetInt32()),
                        };

                        context.Schemas.Add(key, schema);
                    }

                    return JsonSchemaReaderCaseResult.FromSchema(schema);
                }
                else if (type.ValueEquals(JsonSchemaToken.Fixed))
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

                    var schema = new FixedSchema(QualifyName(name.GetString(), scope), size.GetInt32())
                    {
                        LogicalType = new DecimalLogicalType(precision.GetInt32(), scale.ValueKind == JsonValueKind.Undefined ? 0 : scale.GetInt32()),
                    };

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
                    return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonDecimalSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Bytes}\" or \"{JsonSchemaToken.Fixed}\" schemas with the \"{JsonSchemaToken.Decimal}\" logical type."));
                }
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonDecimalSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Bytes}\" or \"{JsonSchemaToken.Fixed}\" schemas with the \"{JsonSchemaToken.Decimal}\" logical type."));
            }
        }
    }
}
