namespace Chr.Avro.Representation
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    /// <summary>
    /// Implements a <see cref="JsonSchemaReader" /> case that matches record schemas.
    /// </summary>
    public class JsonRecordSchemaReaderCase : RecordSchemaReaderCase, IJsonSchemaReaderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonRecordSchemaReaderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to read default values.
        /// </param>
        /// <param name="reader">
        /// A schema reader instance that will be used to read field types.
        /// </param>
        public JsonRecordSchemaReaderCase(IJsonDeserializerBuilder deserializerBuilder, IJsonSchemaReader reader)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Deserializer builder cannot be null.");
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to read default values.
        /// </summary>
        public IJsonDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Gets the schema reader instance that will be used to read field types.
        /// </summary>
        public IJsonSchemaReader Reader { get; }

        /// <summary>
        /// Reads a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSchemaReaderCaseResult" /> with a <see cref="RecordSchema" />
        /// if <paramref name="element" /> is a record schema; an unsuccessful
        /// <see cref="JsonSchemaReaderCaseResult" /> with an <see cref="UnknownSchemaException" />
        /// otherwise.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a fields property is not present on the schema.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSchemaReaderCaseResult Read(JsonElement element, JsonSchemaReaderContext context)
        {
            if (element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueEquals(JsonSchemaToken.Record))
            {
                if (!element.TryGetProperty(JsonAttributeToken.Name, out var name) || name.ValueKind != JsonValueKind.String)
                {
                    throw new InvalidSchemaException($"Named schemas must contain a \"{JsonAttributeToken.Name}\" key.");
                }

                if (!element.TryGetProperty(JsonAttributeToken.Fields, out var fields))
                {
                    throw new InvalidSchemaException($"\"{JsonSchemaToken.Record}\" schemas must contain a \"{JsonAttributeToken.Fields}\" key.");
                }

                var scope = element.TryGetProperty(JsonAttributeToken.Namespace, out var @namespace)
                    ? @namespace.GetString()
                    : context.Scope;

                var schema = new RecordSchema(QualifyName(name.GetString(), scope));

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

                var originalScope = context.Scope;
                context.Scope = scope;

                foreach (JsonElement fieldElement in fields.EnumerateArray())
                {
                    if (!fieldElement.TryGetProperty(JsonAttributeToken.Name, out var fieldName) || fieldName.ValueKind != JsonValueKind.String)
                    {
                        throw new InvalidSchemaException($"Record fields must contain a \"{JsonAttributeToken.Name}\" key.");
                    }

                    if (!fieldElement.TryGetProperty(JsonAttributeToken.Type, out var fieldType))
                    {
                        throw new InvalidSchemaException($"Record fields must contain a \"{JsonAttributeToken.Type}\" key.");
                    }

                    var field = new RecordField(fieldName.GetString(), Reader.Read(fieldType, context));

                    if (fieldElement.TryGetProperty(JsonAttributeToken.Default, out var fieldDefault))
                    {
                        field.Default = new JsonDefaultValue(fieldDefault, field.Type, DeserializerBuilder);
                    }

                    if (fieldElement.TryGetProperty(JsonAttributeToken.Doc, out var fieldDoc))
                    {
                        field.Documentation = fieldDoc.GetString();
                    }

                    schema.Fields.Add(field);
                }

                context.Scope = originalScope;

                return JsonSchemaReaderCaseResult.FromSchema(schema);
            }
            else
            {
                return JsonSchemaReaderCaseResult.FromException(new UnknownSchemaException($"{nameof(JsonRecordSchemaReaderCase)} can only be applied to \"{JsonSchemaToken.Record}\" schemas."));
            }
        }
    }
}
