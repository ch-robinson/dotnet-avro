using Chr.Avro.Abstract;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace Chr.Avro.Representation
{
    /// <summary>
    /// Reads an Avro schema from JSON.
    /// </summary>
    public interface IJsonSchemaReader : ISchemaReader
    {
        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// A JSON-encoded schema.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        /// <returns>
        /// Returns a deserialized schema object.
        /// </returns>
        Schema Read(string schema, ConcurrentDictionary<string, Schema> cache = null, string scope = null);

        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="element">
        /// A JSON element representing a schema.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        /// <returns>
        /// Returns a deserialized schema object.
        /// </returns>
        Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache = null, string scope = null);
    }

    /// <summary>
    /// Reads Avro schemas from specific JSON tokens. Used by <see cref="JsonSchemaReader" /> to
    /// break apart read logic.
    /// </summary>
    public interface IJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        bool IsMatch(JsonElement element);

        /// <summary>
        /// Reads a schema from a JSON element.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope);
    }

    /// <summary>
    /// A customizable JSON schema reader backed by a list of cases.
    /// </summary>
    public class JsonSchemaReader : IJsonSchemaReader
    {
        /// <summary>
        /// A list of cases that the read methods will attempt to apply. If the first case does not
        /// match, the next case will be tested, and so on.
        /// </summary>
        protected readonly IReadOnlyCollection<IJsonSchemaReaderCase> Cases;

        /// <summary>
        /// Creates a new JSON schema reader.
        /// </summary>
        /// <param name="cases">
        /// An optional collection of cases. If no case collection is provided, the default set
        /// will be used.
        /// </param>
        public JsonSchemaReader(IReadOnlyCollection<IJsonSchemaReaderCase> cases = null)
        {
            Cases = cases ?? new List<IJsonSchemaReaderCase>()
            {
                // logical types:
                new DateJsonSchemaReaderCase(),
                new DecimalJsonSchemaReaderCase(),
                new DurationJsonSchemaReaderCase(),
                new MicrosecondTimeJsonSchemaReaderCase(),
                new MicrosecondTimestampJsonSchemaReaderCase(),
                new MillisecondTimeJsonSchemaReaderCase(),
                new MillisecondTimestampJsonSchemaReaderCase(),
                new UuidJsonSchemaReaderCase(),

                // collections:
                new ArrayJsonSchemaReaderCase(this),
                new MapJsonSchemaReaderCase(this),

                // unions:
                new UnionJsonSchemaReaderCase(this),

                // named:
                new EnumJsonSchemaReaderCase(),
                new FixedJsonSchemaReaderCase(),
                new RecordJsonSchemaReaderCase(this),

                // others:
                new DefaultJsonSchemaReaderCase()
            };
        }

        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// A JSON-encoded schema.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        /// <returns>
        /// Returns a deserialized schema object.
        /// </returns>
        public Schema Read(string schema, ConcurrentDictionary<string, Schema> cache = null, string scope = null)
        {
            using (var document = JsonDocument.Parse(schema))
            {
                return Read(document.RootElement, cache, scope);
            }
        }

        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="stream">
        /// The stream to read the serialized schema from. (The stream will not be disposed.)
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        /// <returns>
        /// Returns a deserialized schema object.
        /// </returns>
        public Schema Read(Stream stream, ConcurrentDictionary<string, Schema> cache = null, string scope = null)
        {
            using (var document = JsonDocument.Parse(stream))
            {
                return Read(document.RootElement, cache, scope);
            }
        }

        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="element">
        /// A JSON element representing a schema.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        /// <returns>
        /// Returns a deserialized schema object.
        /// </returns>
        public Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache = null, string scope = null)
        {
            if (cache == null)
            {
                cache = new ConcurrentDictionary<string, Schema>();
            }

            var match = Cases.FirstOrDefault(c => c.IsMatch(element));

            if (match == null)
            {
                throw new UnknownSchemaException($"No schema respresentation case matched {element.ToString()}");
            }

            return match.Read(element, cache, scope);
        }
    }

    /// <summary>
    /// A base JSON schema reader case.
    /// </summary>
    public abstract class JsonSchemaReaderCase : IJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public abstract bool IsMatch(JsonElement element);

        /// <summary>
        /// Reads a schema from a JSON element.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public abstract Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope);

        /// <summary>
        /// Qualifies a name if it’s in a scope.
        /// </summary>
        protected virtual string QualifyName(string name, string scope)
        {
            return name.Contains(".") == false && scope != null
                ? $"{scope}.{name}"
                : name;
        }
    }

    /// <summary>
    /// A JSON schema reader case with shared functions to extract fields from named schemas.
    /// </summary>
    public abstract class NamedJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Extracts fully-qualified aliases from a named schema.
        /// </summary>
        protected virtual ICollection<string> GetQualifiedAliases(JsonElement element, string scope)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Aliases, out var aliases))
            {
                return null;
            }

            if (aliases.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidDataException("An \"aliases\" key must have an array as its value.");
            }

            return aliases
                .EnumerateArray()
                .Select(alias =>
                {
                    if (alias.ValueKind != JsonValueKind.String)
                    {
                        throw new InvalidDataException("An \"aliases\" item must be a string.");
                    }

                    return QualifyName(alias.GetString(), scope);
                })
                .ToList();
        }

        /// <summary>
        /// Extracts the fully-qualified name from a named schema.
        /// </summary>
        protected virtual string GetQualifiedName(JsonElement element, string scope)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Name, out var name))
            {
                throw new InvalidDataException("A named schema must contain a \"name\" key.");
            }

            if (name.ValueKind != JsonValueKind.String)
            {
                throw new InvalidDataException("A \"name\" key must have a string as its value.");
            }

            if (!element.TryGetProperty(JsonAttributeToken.Namespace, out var @namespace))
            {
                return QualifyName(name.GetString(), scope);
            }

            if (@namespace.ValueKind != JsonValueKind.String)
            {
                throw new InvalidDataException("A \"namespace\" key must have a string as its value.");
            }

            return $"{@namespace.GetString()}.{name.GetString()}";
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches array schemas.
    /// </summary>
    public class ArrayJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// A schema reader to use to resolve item types.
        /// </summary>
        protected readonly IJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new array case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve item types.
        /// </param>
        public ArrayJsonSchemaReaderCase(IJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Array;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The array case can only be applied to valid array schema representations.");
            }

            var child = Reader.Read(GetItems(element), cache, scope);
            var key = cache.Single(p => p.Value == child).Key;

            return cache.GetOrAdd($"{JsonSchemaToken.Array}<{key}>", _ => new ArraySchema(child));
        }

        /// <summary>
        /// Extracts the item type from an array schema.
        /// </summary>
        protected virtual JsonElement GetItems(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Items, out var items))
            {
                throw new InvalidDataException("Array schemas must contain an \"items\" key.");
            }

            return items;
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches int schemas with date logical types.
    /// </summary>
    public class DateJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Int
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.Date;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The date case can only be applied to \"int\" schemas with a \"date\" logical type.");
            }

            return cache.GetOrAdd($"{JsonSchemaToken.Int}!{JsonSchemaToken.Date}", _ => new IntSchema()
            {
                LogicalType = new DateLogicalType()
            });
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches bytes or fixed schemas with decimal logical types.
    /// </summary>
    public class DecimalJsonSchemaReaderCase : FixedJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && (type.GetString() == JsonSchemaToken.Bytes || type.GetString() == JsonSchemaToken.Fixed)
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.Decimal;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The decimal case can only be applied to \"bytes\" schemas with a \"decimal\" logical type.");
            }

            if (element.GetProperty(JsonAttributeToken.Type).GetString() == JsonSchemaToken.Fixed)
            {
                var schema = new FixedSchema(GetQualifiedName(element, scope), GetSize(element))
                {
                    Aliases = GetQualifiedAliases(element, scope) ?? new string[0],
                    LogicalType = new DecimalLogicalType(GetPrecision(element), GetScale(element))
                };

                if (!cache.TryAdd(schema.FullName, schema))
                {
                    throw new InvalidDataException($"Invalid fixed name; a definition for {schema.FullName} was already read.");
                }

                foreach (var alias in schema.Aliases)
                {
                    if (!cache.TryAdd(alias, schema))
                    {
                        throw new InvalidDataException($"Invalid fixed alias; a definition for {alias} was already read.");
                    }
                }

                return schema;
            }
            else
            {
                return cache.GetOrAdd($"{JsonSchemaToken.Bytes}!{JsonSchemaToken.Decimal}", new BytesSchema()
                {
                    LogicalType = new DecimalLogicalType(GetPrecision(element), GetScale(element))
                });
            }
        }

        /// <summary>
        /// Extracts the precision from a decimal schema.
        /// </summary>
        protected virtual int GetPrecision(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Precision, out var precision) || precision.ValueKind != JsonValueKind.Number)
            {
                throw new InvalidDataException("Decimal schemas must contain a \"precision\" key with an integer as its value.");
            }

            return precision.GetInt32();
        }

        /// <summary>
        /// Extracts the scale from a decimal schema.
        /// </summary>
        protected virtual int GetScale(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Scale, out var scale) || scale.ValueKind != JsonValueKind.Number)
            {
                throw new InvalidDataException("Decimal schemas must contain a \"scale\" key with an integer as its value.");
            }

            return scale.GetInt32();
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches all unhandled names.
    /// </summary>
    public class DefaultJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                element.TryGetProperty(JsonAttributeToken.Type, out element);
            }

            return element.ValueKind == JsonValueKind.String;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                element.TryGetProperty(JsonAttributeToken.Type, out element);
            }

            if (element.ValueKind != JsonValueKind.String)
            {
                throw new ArgumentException("The primitive case can only be applied to valid primitive schema representations.");
            }

            var type = element.GetString();

            switch (type)
            {
                case JsonSchemaToken.Boolean:
                    return cache.GetOrAdd(type, _ => new BooleanSchema());

                case JsonSchemaToken.Bytes:
                    return cache.GetOrAdd(type, _ => new BytesSchema());

                case JsonSchemaToken.Double:
                    return cache.GetOrAdd(type, _ => new DoubleSchema());

                case JsonSchemaToken.Float:
                    return cache.GetOrAdd(type, _ => new FloatSchema());

                case JsonSchemaToken.Int:
                    return cache.GetOrAdd(type, _ => new IntSchema());

                case JsonSchemaToken.Long:
                    return cache.GetOrAdd(type, _ => new LongSchema());

                case JsonSchemaToken.Null:
                    return cache.GetOrAdd(type, _ => new NullSchema());

                case JsonSchemaToken.String:
                    return cache.GetOrAdd(type, _ => new StringSchema());

                case var name:
                    var qualified = QualifyName(name, scope);

                    if (cache.TryGetValue(qualified, out var match))
                    {
                        return match;
                    }

                    if (name != qualified && cache.TryGetValue(name, out match))
                    {
                        return match;
                    }

                    throw new UnknownSchemaException($"\"{name}\" is not a known schema.");
            }
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches fixed schemas with duration logical types.
    /// </summary>
    public class DurationJsonSchemaReaderCase : FixedJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Fixed
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.Duration;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The duration case can only be applied to \"fixed\" schemas with a \"duration\" logical type.");
            }

            var schema = new FixedSchema(GetQualifiedName(element, scope), GetSize(element))
            {
                Aliases = GetQualifiedAliases(element, scope) ?? new string[0],
                LogicalType = new DurationLogicalType()
            };

            if (!cache.TryAdd(schema.FullName, schema))
            {
                throw new InvalidDataException($"Invalid duration name; a definition for {schema.FullName} was already read.");
            }

            foreach (var alias in schema.Aliases)
            {
                if (!cache.TryAdd(alias, schema))
                {
                    throw new InvalidDataException($"Invalid duration alias; a definition for {alias} was already read.");
                }
            }

            return schema;
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches enum schemas.
    /// </summary>
    public class EnumJsonSchemaReaderCase : NamedJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Enum;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The enum case can only be applied to valid enum schema representations.");
            }

            var schema = new EnumSchema(GetQualifiedName(element, scope), GetSymbols(element))
            {
                Aliases = GetQualifiedAliases(element, scope) ?? new string[0],
                Documentation = GetDoc(element)
            };

            if (!cache.TryAdd(schema.FullName, schema))
            {
                throw new InvalidDataException($"Invalid enum name; a definition for {schema.FullName} was already read.");
            }

            foreach (var alias in schema.Aliases)
            {
                if (!cache.TryAdd(alias, schema))
                {
                    throw new InvalidDataException($"Invalid enum alias; a definition for {alias} was already read.");
                }
            }

            return schema;
        }

        /// <summary>
        /// Extracts the documentation field from an enum schema.
        /// </summary>
        protected virtual string GetDoc(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Doc, out var doc))
            {
                return null;
            }

            if (doc.ValueKind != JsonValueKind.String)
            {
                throw new InvalidDataException("A \"doc\" key must have a string as its value.");
            }

            return doc.GetString();
        }

        /// <summary>
        /// Extracts the symbols from an enum schema.
        /// </summary>
        protected virtual ICollection<string> GetSymbols(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Symbols, out var symbols))
            {
                throw new InvalidDataException("Enum schemas must contain a \"symbols\" key.");
            }

            if (symbols.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidDataException("A \"symbols\" key must have an array as its value.");
            }

            return symbols
                .EnumerateArray()
                .Select(symbol =>
                {
                    if (symbol.ValueKind != JsonValueKind.String)
                    {
                        throw new InvalidDataException("A \"symbols\" item must be a string.");
                    }

                    return symbol.GetString();
                })
                .ToList();
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches fixed schemas.
    /// </summary>
    public class FixedJsonSchemaReaderCase : NamedJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Fixed;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The fixed case can only be applied to valid fixed schema representations.");
            }

            var schema = new FixedSchema(GetQualifiedName(element, scope), GetSize(element))
            {
                Aliases = GetQualifiedAliases(element, scope) ?? new string[0]
            };

            if (!cache.TryAdd(schema.FullName, schema))
            {
                throw new InvalidDataException($"Invalid fixed name; a definition for {schema.FullName} was already read.");
            }

            foreach (var alias in schema.Aliases)
            {
                if (!cache.TryAdd(alias, schema))
                {
                    throw new InvalidDataException($"Invalid fixed alias; a definition for {alias} was already read.");
                }
            }

            return schema;
        }

        /// <summary>
        /// Extracts the size from a fixed schema.
        /// </summary>
        protected virtual int GetSize(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Size, out var size) || size.ValueKind != JsonValueKind.Number)
            {
                throw new InvalidDataException("Fixed schemas must contain a \"size\" key with an integer as its value.");
            }

            return size.GetInt32();
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches map schemas.
    /// </summary>
    public class MapJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// A schema reader to use to resolve value types.
        /// </summary>
        protected readonly IJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new map case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve item types.
        /// </param>
        public MapJsonSchemaReaderCase(IJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Map;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The map case can only be applied to valid map schema representations.");
            }

            var child = Reader.Read(GetValues(element), cache, scope);
            var key = cache.Single(p => p.Value == child).Key;

            return cache.GetOrAdd($"{JsonSchemaToken.Map}<{key}>", _ => new MapSchema(child));
        }

        /// <summary>
        /// Extracts the value type from a map schema.
        /// </summary>
        protected virtual JsonElement GetValues(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Values, out var values))
            {
                throw new InvalidDataException("Map schemas must contain a \"values\" key.");
            }

            return values;
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches long schemas with microsecond time logical types.
    /// </summary>
    public class MicrosecondTimeJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Long
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.TimeMicroseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The microsecond time case can only be applied to \"long\" schemas with a \"time-micros\" logical type.");
            }

            return cache.GetOrAdd($"{JsonSchemaToken.Long}!{JsonSchemaToken.TimeMicroseconds}", _ => new LongSchema()
            {
                LogicalType = new MicrosecondTimeLogicalType()
            });
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches long schemas with microsecond timestamp logical types.
    /// </summary>
    public class MicrosecondTimestampJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Long
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.TimestampMicroseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The microsecond timestamp case can only be applied to \"long\" schemas with a \"timestamp-micros\" logical type.");
            }

            return cache.GetOrAdd($"{JsonSchemaToken.Long}!{JsonSchemaToken.TimestampMicroseconds}", _ => new LongSchema()
            {
                LogicalType = new MicrosecondTimestampLogicalType()
            });
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches int schemas with millisecond time logical types.
    /// </summary>
    public class MillisecondTimeJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Int
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.TimeMilliseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The millisecond time case can only be applied to \"int\" schemas with a \"time-millis\" logical type.");
            }

            return cache.GetOrAdd($"{JsonSchemaToken.Int}!{JsonSchemaToken.TimeMilliseconds}", _ => new IntSchema()
            {
                LogicalType = new MillisecondTimeLogicalType()
            });
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches long schemas with microsecond time logical types.
    /// </summary>
    public class MillisecondTimestampJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Long
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.TimestampMilliseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The millisecond time case can only be applied to \"long\" schemas with a \"timestamp-millis\" logical type.");
            }

            return cache.GetOrAdd($"{JsonSchemaToken.Long}!{JsonSchemaToken.TimestampMilliseconds}", _ => new LongSchema()
            {
                LogicalType = new MillisecondTimestampLogicalType()
            });
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches record schemas.
    /// </summary>
    public class RecordJsonSchemaReaderCase : NamedJsonSchemaReaderCase
    {
        /// <summary>
        /// A schema reader to use to resolve field types.
        /// </summary>
        protected readonly IJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new record case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve field types.
        /// </param>
        public RecordJsonSchemaReaderCase(IJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.Record;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The record case can only be applied to valid record schema representations.");
            }

            var schema = new RecordSchema(GetQualifiedName(element, scope))
            {
                Aliases = GetQualifiedAliases(element, scope) ?? new string[0],
                Documentation = GetDoc(element)
            };

            var fields = GetFields(element);

            if (!cache.TryAdd(schema.FullName, schema))
            {
                throw new InvalidDataException($"Invalid record name; a definition for {schema.FullName} was already read.");
            }

            foreach (var alias in schema.Aliases)
            {
                if (!cache.TryAdd(alias, schema))
                {
                    throw new InvalidDataException($"Invalid record alias; a definition for {alias} was already read.");
                }
            }

            foreach (JsonElement field in fields)
            {
                var type = Reader.Read(GetType(field), cache, schema.Namespace);

                schema.Fields.Add(new RecordField(GetName(field), type)
                {
                    Documentation = GetDoc(field)
                });
            }

            return schema;
        }

        /// <summary>
        /// Extracts the documentation field from a record schema.
        /// </summary>
        protected virtual string GetDoc(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Doc, out var doc))
            {
                return null;
            }

            if (doc.ValueKind != JsonValueKind.String)
            {
                throw new InvalidDataException("A \"doc\" key must have a string as its value.");
            }

            return doc.GetString();
        }

        /// <summary>
        /// Extracts the fields from a record schema.
        /// </summary>
        protected virtual IEnumerable<JsonElement> GetFields(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Fields, out var fields) || fields.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidDataException("Record schemas must contain an \"fields\" key with an array as its value.");
            }

            return fields
                .EnumerateArray()
                .Select(field =>
                {
                    if (field.ValueKind != JsonValueKind.Object)
                    {
                        throw new InvalidDataException("A \"fields\" item must be an object.");
                    }

                    return field;
                })
                .ToList();
        }

        /// <summary>
        /// Extracts the name from a record field.
        /// </summary>
        protected virtual string GetName(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Name, out var name) || name.ValueKind != JsonValueKind.String)
            {
                throw new InvalidDataException("Record fields must contain a \"name\" key with an string as its value.");
            }

            return name.GetString();
        }

        /// <summary>
        /// Extracts the type from a record field.
        /// </summary>
        protected virtual JsonElement GetType(JsonElement element)
        {
            if (!element.TryGetProperty(JsonAttributeToken.Type, out var type))
            {
                throw new InvalidDataException("Record fields must contain a \"type\" key.");
            }

            return type;
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches union schemas.
    /// </summary>
    public class UnionJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// A schema reader to use to resolve child types.
        /// </summary>
        protected readonly IJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new union case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve child types.
        /// </param>
        public UnionJsonSchemaReaderCase(IJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Array;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The union case can only be applied to valid union schema representations.");
            }

            var children = element
                .EnumerateArray()
                .Select(child => Reader.Read(child, cache, scope))
                .ToList();

            var keys = children
                .Select(s => cache.Single(p => p.Value == s).Key);

            return cache.GetOrAdd($"[{string.Join(",", keys)}]", _ => new UnionSchema(children));
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches string schemas with UUID logical types.
    /// </summary>
    public class UuidJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to an element.
        /// </summary>
        public override bool IsMatch(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty(JsonAttributeToken.Type, out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() == JsonSchemaToken.String
                && element.TryGetProperty(JsonAttributeToken.LogicalType, out var logicalType)
                && logicalType.ValueKind == JsonValueKind.String
                && logicalType.GetString() == JsonSchemaToken.Uuid;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="element">
        /// The element to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JsonElement element, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(element))
            {
                throw new ArgumentException("The UUID case can only be applied to \"string\" schemas with a \"uuid\" logical type.");
            }

            return cache.GetOrAdd($"{JsonSchemaToken.String}!{JsonSchemaToken.Uuid}", _ => new StringSchema()
            {
                LogicalType = new UuidLogicalType()
            });
        }
    }
}
