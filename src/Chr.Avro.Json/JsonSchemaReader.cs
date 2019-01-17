using Chr.Avro.Abstract;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

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
    }

    /// <summary>
    /// Reads Avro schemas from specific JSON tokens. Used by <see cref="JsonSchemaReader" /> to
    /// break apart read logic.
    /// </summary>
    public interface IJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        bool IsMatch(JToken token);

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope);
    }

    /// <summary>
    /// Reads an Avro schema from JSON using <see cref="Newtonsoft.Json" /> components.
    /// </summary>
    public interface INewtonsoftJsonSchemaReader : IJsonSchemaReader
    {
        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="token">
        /// A JSON token representing a schema.
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
        Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache = null, string scope = null);
    }

    /// <summary>
    /// A customizable JSON schema reader backed by a list of cases.
    /// </summary>
    public class JsonSchemaReader : INewtonsoftJsonSchemaReader
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
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(schema)))
            {
                return Read(stream, cache, scope);
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
            using (var reader = new StreamReader(stream, Encoding.UTF8, true, 1024, true))
            using (var json = new JsonTextReader(reader))
            {
                return Read(JToken.Load(json), cache, scope);
            }
        }

        /// <summary>
        /// Reads a serialized Avro schema.
        /// </summary>
        /// <param name="token">
        /// A JSON token representing a schema.
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
        public Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache = null, string scope = null)
        {
            if (cache == null)
            {
                cache = new ConcurrentDictionary<string, Schema>();
            }

            var match = Cases.FirstOrDefault(c => c.IsMatch(token));

            if (match == null)
            {
                throw new UnknownSchemaException($"No schema respresentation case matched {token.ToString()}");
            }

            return match.Read(token, cache, scope);
        }
    }

    /// <summary>
    /// A base JSON schema reader case.
    /// </summary>
    public abstract class JsonSchemaReaderCase : IJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public abstract bool IsMatch(JToken token);

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public abstract Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope);

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
        protected virtual ICollection<string> GetQualifiedAliases(JObject @object, string scope)
        {
            if (!(@object[JsonAttributeToken.Aliases] is JToken aliases))
            {
                return null;
            }

            if (!(aliases is JArray && aliases.All(t => t.Type == JTokenType.String)))
            {
                throw new InvalidDataException("An \"aliases\" key must have an array of strings as its value.");
            }

            return aliases.Select(alias => QualifyName((string)alias, scope)).ToList();
        }

        /// <summary>
        /// Extracts the fully-qualified name from a named schema.
        /// </summary>
        protected virtual string GetQualifiedName(JObject @object, string scope)
        {
            if (!(@object[JsonAttributeToken.Name] is JValue name && name.Type == JTokenType.String))
            {
                throw new InvalidDataException("A named schema must contain a \"name\" key with a string as its value.");
            }

            if (!(@object[JsonAttributeToken.Namespace] is JToken @namespace))
            {
                return QualifyName((string)name, scope);
            }

            if (!(@namespace is JValue && @namespace.Type == JTokenType.String))
            {
                throw new InvalidDataException("A \"namespace\" key must have a string as its value.");
            }

            return $"{(string)@namespace}.{(string)name}";
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
        protected readonly INewtonsoftJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new array case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve item types.
        /// </param>
        public ArrayJsonSchemaReaderCase(INewtonsoftJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Array;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JObject @object && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Array))
            {
                throw new ArgumentException("The array case can only be applied to valid array schema representations.");
            }

            var child = Reader.Read(GetItems(@object), cache, scope);
            var key = cache.Single(p => p.Value == child).Key;

            return cache.GetOrAdd($"{JsonSchemaToken.Array}<{key}>", _ => new ArraySchema(child));
        }

        /// <summary>
        /// Extracts the item type from an array schema.
        /// </summary>
        protected virtual JToken GetItems(JObject @object)
        {
            if (!(@object[JsonAttributeToken.Items] is JToken items))
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object
                && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Int
                && (string)@object[JsonAttributeToken.LogicalType] == JsonSchemaToken.Date;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(token))
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object
                && ((string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Bytes || (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Fixed)
                && (string)@object[JsonAttributeToken.LogicalType] == JsonSchemaToken.Decimal;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JObject @object && IsMatch(@object)))
            {
                throw new ArgumentException("The decimal case can only be applied to \"bytes\" schemas with a \"decimal\" logical type.");
            }
            
            if ((string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Fixed)
            {
                var schema = new FixedSchema(GetQualifiedName(@object, scope), GetSize(@object))
                {
                    Aliases = GetQualifiedAliases(@object, scope) ?? new string[0],
                    LogicalType = new DecimalLogicalType(GetPrecision(@object), GetScale(@object))
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
                    LogicalType = new DecimalLogicalType(GetPrecision(@object), GetScale(@object))
                });
            }
        }

        /// <summary>
        /// Extracts the precision from a decimal schema.
        /// </summary>
        protected virtual int GetPrecision(JToken @object)
        {
            if (!(@object[JsonAttributeToken.Precision] is JValue precision && precision.Type == JTokenType.Integer))
            {
                throw new InvalidDataException("Decimal schemas must contain a \"precision\" key with an integer as its value.");
            }

            return (int)precision;
        }

        /// <summary>
        /// Extracts the scale from a decimal schema.
        /// </summary>
        protected virtual int GetScale(JToken @object)
        {
            if (!(@object[JsonAttributeToken.Scale] is JValue scale && scale.Type == JTokenType.Integer))
            {
                throw new InvalidDataException("Decimal schemas must contain a \"scale\" key with an integer as its value.");
            }

            return (int)scale;
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches all unhandled names.
    /// </summary>
    public class DefaultJsonSchemaReaderCase : JsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            if (token is JObject @object)
            {
                token = @object[JsonAttributeToken.Type];
            }

            return token is JValue value && value.Type == JTokenType.String;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (token is JObject @object)
            {
                token = @object[JsonAttributeToken.Type];
            }

            if (!(token is JValue value && value.Type == JTokenType.String))
            {
                throw new ArgumentException("The primitive case can only be applied to valid primitive schema representations.");
            }

            var type = (string)value;
            
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object
                && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Fixed
                && (string)@object[JsonAttributeToken.LogicalType] == JsonSchemaToken.Duration;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JObject @object && IsMatch(@object)))
            {
                throw new ArgumentException("The duration case can only be applied to \"fixed\" schemas with a \"duration\" logical type.");
            }
            
            var schema = new FixedSchema(GetQualifiedName(@object, scope), GetSize(@object))
            {
                Aliases = GetQualifiedAliases(@object, scope) ?? new string[0],
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Enum;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JObject @object && (string)token[JsonAttributeToken.Type] == JsonSchemaToken.Enum))
            {
                throw new ArgumentException("The enum case can only be applied to valid enum schema representations.");
            }

            var schema = new EnumSchema(GetQualifiedName(@object, scope), GetSymbols(@object))
            {
                Aliases = GetQualifiedAliases(@object, scope) ?? new string[0],
                Documentation = GetDoc(@object)
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
        protected virtual string GetDoc(JToken @object)
        {
            if (!(@object[JsonAttributeToken.Doc] is JToken doc))
            {
                return null;
            }

            if (!(doc is JValue && doc.Type == JTokenType.String))
            {
                throw new InvalidDataException("A \"doc\" key must have a string as its value.");
            }

            return (string)doc;
        }

        /// <summary>
        /// Extracts the symbols from an enum schema.
        /// </summary>
        protected virtual ICollection<string> GetSymbols(JToken @object)
        {
            if (!(@object[JsonAttributeToken.Symbols] is JArray symbols && symbols.All(s => s.Type == JTokenType.String)))
            {
                throw new InvalidDataException("Enum schemas must contain a \"symbols\" key with an array of strings as its value.");
            }

            return symbols.Select(s => (string)s).ToList();
        }
    }

    /// <summary>
    /// A JSON schema reader case that matches fixed schemas.
    /// </summary>
    public class FixedJsonSchemaReaderCase : NamedJsonSchemaReaderCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Fixed;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JObject @object && (string)token[JsonAttributeToken.Type] == JsonSchemaToken.Fixed))
            {
                throw new ArgumentException("The fixed case can only be applied to valid fixed schema representations.");
            }

            var schema = new FixedSchema(GetQualifiedName(@object, scope), GetSize(@object))
            {
                Aliases = GetQualifiedAliases(@object, scope) ?? new string[0]
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
        protected virtual int GetSize(JToken @object)
        {
            if (!(@object[JsonAttributeToken.Size] is JValue size && size.Type == JTokenType.Integer))
            {
                throw new InvalidDataException("Fixed schemas must contain a \"size\" key with an integer as its value.");
            }

            return (int)size;
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
        protected readonly INewtonsoftJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new map case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve item types.
        /// </param>
        public MapJsonSchemaReaderCase(INewtonsoftJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Map;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JObject @object && (string)token[JsonAttributeToken.Type] == JsonSchemaToken.Map))
            {
                throw new ArgumentException("The map case can only be applied to valid map schema representations.");
            }
            
            var child = Reader.Read(GetValues(@object), cache, scope);
            var key = cache.Single(p => p.Value == child).Key;

            return cache.GetOrAdd($"{JsonSchemaToken.Map}<{key}>", _ => new MapSchema(child));
        }

        /// <summary>
        /// Extracts the value type from a map schema.
        /// </summary>
        protected virtual JToken GetValues(JObject @object)
        {
            if (!(@object[JsonAttributeToken.Values] is JToken values))
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object
                && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Long
                && (string)@object[JsonAttributeToken.LogicalType] == JsonSchemaToken.TimeMicroseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(token))
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object
                && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Long
                && (string)@object[JsonAttributeToken.LogicalType] == JsonSchemaToken.TimestampMicroseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(token))
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object
                && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Int
                && (string)@object[JsonAttributeToken.LogicalType] == JsonSchemaToken.TimeMilliseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(token))
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
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object
                && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Long
                && (string)@object[JsonAttributeToken.LogicalType] == JsonSchemaToken.TimestampMilliseconds;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!IsMatch(token))
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
        protected readonly INewtonsoftJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new record case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve field types.
        /// </param>
        public RecordJsonSchemaReaderCase(INewtonsoftJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JObject @object && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Record;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JObject @object && (string)@object[JsonAttributeToken.Type] == JsonSchemaToken.Record))
            {
                throw new ArgumentException("The record case can only be applied to valid record schema representations.");
            }

            var schema = new RecordSchema(GetQualifiedName(@object, scope))
            {
                Aliases = GetQualifiedAliases(@object, scope) ?? new string[0],
                Documentation = GetDoc(@object)
            };

            var fields = GetFields(@object);

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

            foreach (JObject field in fields)
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
        protected virtual string GetDoc(JToken @object)
        {
            if (!(@object[JsonAttributeToken.Doc] is JToken doc))
            {
                return null;
            }

            if (!(doc is JValue && doc.Type == JTokenType.String))
            {
                throw new InvalidDataException("A \"doc\" key must have a string as its value.");
            }

            return (string)doc;
        }

        /// <summary>
        /// Extracts the fields from a record schema.
        /// </summary>
        protected virtual JArray GetFields(JObject @object)
        {
            if (!(@object[JsonAttributeToken.Fields] is JArray fields && fields.All(f => f is JObject)))
            {
                throw new InvalidDataException("Record schemas must contain an \"fields\" key with an array as its value.");
            }

            return fields;
        }

        /// <summary>
        /// Extracts the name from a record field.
        /// </summary>
        protected virtual string GetName(JObject @object)
        {
            if (!(@object[JsonAttributeToken.Name] is JValue name && name.Type == JTokenType.String))
            {
                throw new InvalidDataException("Record fields must contain a \"name\" key with an string as its value.");
            }

            return (string)name;
        }

        /// <summary>
        /// Extracts the type from a record field.
        /// </summary>
        protected virtual JToken GetType(JObject @object)
        {
            if (!(@object[JsonAttributeToken.Type] is JToken type))
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
        protected readonly INewtonsoftJsonSchemaReader Reader;

        /// <summary>
        /// Creates a new union case.
        /// </summary>
        /// <param name="reader">
        /// A schema reader to use to resolve child types.
        /// </param>
        public UnionJsonSchemaReaderCase(INewtonsoftJsonSchemaReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader), "Schema reader cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a token.
        /// </summary>
        public override bool IsMatch(JToken token)
        {
            return token is JArray;
        }

        /// <summary>
        /// Reads a schema from a JSON token.
        /// </summary>
        /// <param name="token">
        /// The token to parse.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache is populated as the schema is read and can be used
        /// to provide schemas for certain names or cache keys.
        /// </param>
        /// <param name="scope">
        /// The surrounding namespace, if any.
        /// </param>
        public override Schema Read(JToken token, ConcurrentDictionary<string, Schema> cache, string scope)
        {
            if (!(token is JArray array))
            {
                throw new ArgumentException("The union case can only be applied to valid union schema representations.");
            }

            var children = array.Children()
                .Select(c => Reader.Read(c, cache, scope))
                .ToList();

            var keys = children
                .Select(s => cache.Single(p => p.Value == s).Key);

            return cache.GetOrAdd($"[{string.Join(",", keys)}]", _ => new UnionSchema(children));
        }
    }
}
