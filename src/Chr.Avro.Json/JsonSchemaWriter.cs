using Chr.Avro.Abstract;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Chr.Avro.Representation
{
    /// <summary>
    /// Writes an Avro schema to JSON.
    /// </summary>
    public interface IJsonSchemaWriter : ISchemaWriter
    {
        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// An optional schema cache. The cache is populated as the schema is written and can be
        /// used to determine which named schemas have already been processed.
        /// </param>
        /// <returns>
        /// Returns a JSON-encoded schema.
        /// </returns>
        string Write(Schema schema, bool canonical = false, ConcurrentDictionary<string, NamedSchema> names = null);

        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The writer to use for JSON operations.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// An optional schema cache. The cache is populated as the schema is written and can be
        /// used to determine which named schemas have already been processed.
        /// </param>
        void Write(Schema schema, Utf8JsonWriter json, bool canonical = false, ConcurrentDictionary<string, NamedSchema> names = null);
    }

    /// <summary>
    /// Writes specific Avro schemas to JSON. Used by <see cref="JsonSchemaWriter" /> to break apart
    /// write logic.
    /// </summary>
    public interface IJsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        bool IsMatch(Schema schema);

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names);
    }

    /// <summary>
    /// A customizable JSON schema writer backed by a list of cases.
    /// </summary>
    public class JsonSchemaWriter : IJsonSchemaWriter
    {
        /// <summary>
        /// A list of cases that the write methods will attempt to apply. If the first case does
        /// not match, the next case will be tested, and so on.
        /// </summary>
        protected readonly IReadOnlyCollection<IJsonSchemaWriterCase> Cases;

        /// <summary>
        /// Creates a new JSON schema writer.
        /// </summary>
        /// <param name="cases">
        /// An optional collection of cases. If no case collection is provided, the default set
        /// will be used.
        /// </param>
        public JsonSchemaWriter(IReadOnlyCollection<IJsonSchemaWriterCase> cases = null)
        {
            Cases = cases ?? new List<IJsonSchemaWriterCase>()
            {
                // logical types:
                new DateJsonSchemaWriterCase(),
                new DecimalJsonSchemaWriterCase(),
                new DurationJsonSchemaWriterCase(),
                new MicrosecondTimeJsonSchemaWriterCase(),
                new MicrosecondTimestampJsonSchemaWriterCase(),
                new MillisecondTimeJsonSchemaWriterCase(),
                new MillisecondTimestampJsonSchemaWriterCase(),
                new UuidJsonSchemaWriterCase(),

                // collections:
                new ArrayJsonSchemaWriterCase(this),
                new MapJsonSchemaWriterCase(this),

                // unions:
                new UnionJsonSchemaWriterCase(this),

                // named:
                new EnumJsonSchemaWriterCase(),
                new FixedJsonSchemaWriterCase(),
                new RecordJsonSchemaWriterCase(this),

                // primitives:
                new PrimitiveJsonSchemaWriterCase()
            };
        }

        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// An optional schema cache. The cache is populated as the schema is written and can be
        /// used to determine which named schemas have already been processed.
        /// </param>
        /// <returns>
        /// Returns a JSON-encoded schema.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a schema constraint prevents a valid schema from being
        /// written.
        /// </exception>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when no matching case is found for the schema.
        /// </exception>
        public virtual string Write(Schema schema, bool canonical = false, ConcurrentDictionary<string, NamedSchema> names = null)
        {
            using (var stream = new MemoryStream())
            using (var reader = new StreamReader(stream))
            {
                Write(schema, stream, canonical, names);

                stream.Seek(0, SeekOrigin.Begin);
                return reader.ReadToEnd();
            }
        }

        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="stream">
        /// The stream to write the schema to. (The stream will not be disposed.)
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// An optional schema cache. The cache is populated as the schema is written and can be
        /// used to determine which named schemas have already been processed.
        /// </param>
        /// <returns>
        /// Returns a JSON-encoded schema.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a schema constraint prevents a valid schema from being
        /// written.
        /// </exception>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when no matching case is found for the schema.
        /// </exception>
        public virtual void Write(Schema schema, Stream stream, bool canonical = false, ConcurrentDictionary<string, NamedSchema> names = null)
        {
            using (var json = new Utf8JsonWriter(stream))
            {
                Write(schema, json, canonical, names);
            }
        }

        /// <summary>
        /// Writes a serialized Avro schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The writer to use for JSON operations.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// An optional schema cache. The cache is populated as the schema is written and can be
        /// used to determine which named schemas have already been processed.
        /// </param>
        /// <returns>
        /// Returns a JSON-encoded schema.
        /// </returns>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a schema constraint prevents a valid schema from being
        /// written.
        /// </exception>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when no matching case is found for the schema.
        /// </exception>
        public virtual void Write(Schema schema, Utf8JsonWriter json, bool canonical = false, ConcurrentDictionary<string, NamedSchema> names = null)
        {
            if (names == null)
            {
                names = new ConcurrentDictionary<string, NamedSchema>();
            }

            var match = Cases.FirstOrDefault(c => c.IsMatch(schema));

            if (match == null)
            {
                throw new UnsupportedSchemaException(schema, $"No schema representation case matched {schema.GetType().Name}.");
            }

            match.Write(schema, json, canonical, names);
        }
    }

    /// <summary>
    /// A base JSON schema writer case.
    /// </summary>
    public abstract class JsonSchemaWriterCase : IJsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public abstract bool IsMatch(Schema schema);

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public abstract void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names);
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="ArraySchema" />.
    /// </summary>
    public class ArrayJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// A schema writer to use to write item schemas.
        /// </summary>
        protected readonly IJsonSchemaWriter Writer;

        /// <summary>
        /// Creates a new array case.
        /// </summary>
        /// <param name="writer">
        /// A schema writer to use to write item schemas.
        /// </param>
        public ArrayJsonSchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is ArraySchema;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is ArraySchema arraySchema))
            {
                throw new ArgumentException("The array case can only be applied to an array schema.");
            }

            json.WriteStartObject();
            json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Array);
            json.WritePropertyName(JsonAttributeToken.Items);
            Writer.Write(arraySchema.Item, json, canonical, names);
            json.WriteEndObject();
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="DateLogicalType" />.
    /// </summary>
    public class DateJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is IntSchema && schema.LogicalType is DateLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!IsMatch(schema))
            {
                throw new ArgumentException("The date case can only be applied to an int schema with a date logical type.");
            }


            if (canonical)
            {
                json.WriteStringValue(JsonSchemaToken.Int);
            }
            else
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Int);
                json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Date);
                json.WriteEndObject();
            }

        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="DecimalLogicalType" />.
    /// </summary>
    public class DecimalJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return (schema is BytesSchema || schema is FixedSchema)
                && schema.LogicalType is DecimalLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema.LogicalType is DecimalLogicalType decimalLogicalType))
            {
                throw new ArgumentException("The decimal case can only be applied to bytes or fixed schemas with a decimal logical type.");
            }

            if (schema is FixedSchema fixedSchema)
            {
                if (names.TryGetValue(fixedSchema.FullName, out var existing))
                {
                    if (!schema.Equals(existing))
                    {
                        throw new InvalidSchemaException($"A conflicting schema with the name {fixedSchema.FullName} has already been written.");
                    }

                    json.WriteStringValue(fixedSchema.FullName);
                    return;
                }

                if (!names.TryAdd(fixedSchema.FullName, fixedSchema))
                {
                    throw new InvalidOperationException();
                }

                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Name, fixedSchema.FullName);

                if (!canonical)
                {
                    if (fixedSchema.Aliases.Count > 0)
                    {
                        json.WritePropertyName(JsonAttributeToken.Aliases);
                        json.WriteStartArray();

                        foreach (var alias in fixedSchema.Aliases)
                        {
                            json.WriteStringValue(alias);
                        }

                        json.WriteEndArray();
                    }
                }

                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Fixed);

                if (!canonical)
                {
                    json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Decimal);
                    json.WriteNumber(JsonAttributeToken.Precision, decimalLogicalType.Precision);
                    json.WriteNumber(JsonAttributeToken.Scale, decimalLogicalType.Scale);
                }

                json.WriteNumber(JsonAttributeToken.Size, fixedSchema.Size);
                json.WriteEndObject();
            }
            else if (schema is BytesSchema)
            {

                if (canonical)
                {
                    json.WriteStringValue(JsonSchemaToken.Bytes);
                }
                else
                {
                    json.WriteStartObject();
                    json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Bytes);
                    json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Decimal);
                    json.WriteNumber(JsonAttributeToken.Precision, decimalLogicalType.Precision);
                    json.WriteNumber(JsonAttributeToken.Scale, decimalLogicalType.Scale);
                    json.WriteEndObject();
                }
            }
            else
            {
                throw new ArgumentException("The decimal case can only be applied to bytes or fixed schemas with a decimal logical type.");
            }
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="DurationLogicalType" />.
    /// </summary>
    public class DurationJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is FixedSchema && schema.LogicalType is DurationLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is FixedSchema fixedSchema && schema.LogicalType is DurationLogicalType))
            {
                throw new ArgumentException("The duration case can only be applied to a fixed schema with a duration logical type.");
            }

            if (names.TryGetValue(fixedSchema.FullName, out var existing))
            {
                if (!schema.Equals(existing))
                {
                    throw new InvalidSchemaException($"A conflicting schema with the name {fixedSchema.FullName} has already been written.");
                }

                json.WriteStringValue(fixedSchema.FullName);
                return;
            }

            if (!names.TryAdd(fixedSchema.FullName, fixedSchema))
            {
                throw new InvalidOperationException();
            }

            json.WriteStartObject();
            json.WriteString(JsonAttributeToken.Name, fixedSchema.FullName);

            if (!canonical)
            {
                if (fixedSchema.Aliases.Count > 0)
                {
                    json.WritePropertyName(JsonAttributeToken.Aliases);
                    json.WriteStartArray();

                    foreach (var alias in fixedSchema.Aliases)
                    {
                        json.WriteStringValue(alias);
                    }

                    json.WriteEndArray();
                }
            }

            json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Fixed);

            if (!canonical)
            {
                json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Duration);
            }

            json.WriteNumber(JsonAttributeToken.Size, fixedSchema.Size);
            json.WriteEndObject();
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="EnumSchema" />.
    /// </summary>
    public class EnumJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is EnumSchema;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is EnumSchema enumSchema))
            {
                throw new ArgumentException("The enum case can only be applied to an enum schema.");
            }

            if (names.TryGetValue(enumSchema.FullName, out var existing))
            {
                if (!schema.Equals(existing))
                {
                    throw new InvalidSchemaException($"A conflicting schema with the name {enumSchema.FullName} has already been written.");
                }

                json.WriteStringValue(enumSchema.FullName);
                return;
            }

            if (!names.TryAdd(enumSchema.FullName, enumSchema))
            {
                throw new InvalidOperationException();
            }

            json.WriteStartObject();
            json.WriteString(JsonAttributeToken.Name, enumSchema.FullName);

            if (!canonical)
            {
                if (enumSchema.Aliases.Count > 0)
                {
                    json.WritePropertyName(JsonAttributeToken.Aliases);
                    json.WriteStartArray();

                    foreach (var alias in enumSchema.Aliases)
                    {
                        json.WriteStringValue(alias);
                    }

                    json.WriteEndArray();
                }

                if (!string.IsNullOrEmpty(enumSchema.Documentation))
                {
                    json.WriteString(JsonAttributeToken.Doc, enumSchema.Documentation);
                }
            }

            json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Enum);
            json.WritePropertyName(JsonAttributeToken.Symbols);
            json.WriteStartArray();

            foreach (var symbol in enumSchema.Symbols)
            {
                json.WriteStringValue(symbol);
            }

            json.WriteEndArray();
            json.WriteEndObject();
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="FixedSchema" />.
    /// </summary>
    public class FixedJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is FixedSchema;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is FixedSchema fixedSchema))
            {
                throw new ArgumentException("The fixed case can only be applied to a fixed schema.");
            }

            if (names.TryGetValue(fixedSchema.FullName, out var existing))
            {
                if (!schema.Equals(existing))
                {
                    throw new InvalidSchemaException($"A conflicting schema with the name {fixedSchema.FullName} has already been written.");
                }

                json.WriteStringValue(fixedSchema.FullName);
                return;
            }

            if (!names.TryAdd(fixedSchema.FullName, fixedSchema))
            {
                throw new InvalidOperationException();
            }

            json.WriteStartObject();
            json.WriteString(JsonAttributeToken.Name, fixedSchema.FullName);

            if (!canonical)
            {
                if (fixedSchema.Aliases.Count > 0)
                {
                    json.WritePropertyName(JsonAttributeToken.Aliases);
                    json.WriteStartArray();

                    foreach (var alias in fixedSchema.Aliases)
                    {
                        json.WriteStringValue(alias);
                    }

                    json.WriteEndArray();
                }
            }

            json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Fixed);
            json.WriteNumber(JsonAttributeToken.Size, fixedSchema.Size);
            json.WriteEndObject();
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="MapSchema" />.
    /// </summary>
    public class MapJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// A schema writer to use to write value schemas.
        /// </summary>
        protected readonly IJsonSchemaWriter Writer;

        /// <summary>
        /// Creates a new map case.
        /// </summary>
        /// <param name="writer">
        /// A schema writer to use to write value schemas.
        /// </param>
        public MapJsonSchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is MapSchema;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is MapSchema mapSchema))
            {
                throw new ArgumentException("The map case can only be applied to a map schema.");
            }

            json.WriteStartObject();
            json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Map);
            json.WritePropertyName(JsonAttributeToken.Values);
            Writer.Write(mapSchema.Value, json, canonical, names);
            json.WriteEndObject();
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="MicrosecondTimeLogicalType" />.
    /// </summary>
    public class MicrosecondTimeJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is LongSchema && schema.LogicalType is MicrosecondTimeLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!IsMatch(schema))
            {
                throw new ArgumentException("The microsecond time case can only be applied to a long schema with a microsecond time logical type.");
            }


            if (canonical)
            {
                json.WriteStringValue(JsonSchemaToken.Long);
            }
            else
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Long);
                json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.TimeMicroseconds);
                json.WriteEndObject();
            }
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="MicrosecondTimestampLogicalType" />.
    /// </summary>
    public class MicrosecondTimestampJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is LongSchema && schema.LogicalType is MicrosecondTimestampLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!IsMatch(schema))
            {
                throw new ArgumentException("The microsecond timestamp case can only be applied to a long schema with a microsecond timestamp logical type.");
            }

            if (canonical)
            {
                json.WriteStringValue(JsonSchemaToken.Long);
            }
            else
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Long);
                json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.TimestampMicroseconds);
                json.WriteEndObject();
            }
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="MillisecondTimeLogicalType" />.
    /// </summary>
    public class MillisecondTimeJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is IntSchema && schema.LogicalType is MillisecondTimeLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!IsMatch(schema))
            {
                throw new ArgumentException("The millisecond time case can only be applied to an int schema with a millisecond time logical type.");
            }

            if (canonical)
            {
                json.WriteStringValue(JsonSchemaToken.Int);
            }
            else
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Int);
                json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.TimeMilliseconds);
                json.WriteEndObject();
            }
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="MillisecondTimestampLogicalType" />.
    /// </summary>
    public class MillisecondTimestampJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is LongSchema && schema.LogicalType is MillisecondTimestampLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!IsMatch(schema))
            {
                throw new ArgumentException("The millisecond timestamp case can only be applied to a long schema with a millisecond timestamp logical type.");
            }

            if (canonical)
            {
                json.WriteStringValue(JsonSchemaToken.Long);
            }
            else
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Long);
                json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.TimestampMilliseconds);
                json.WriteEndObject();
            }
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches all <see cref="PrimitiveSchema" /> subclasses.
    /// </summary>
    public class PrimitiveJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is PrimitiveSchema;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is PrimitiveSchema primitiveSchema))
            {
                throw new ArgumentException("The primitive case can only be applied to a primitive schema.");
            }

            json.WriteStringValue(GetSchemaToken(primitiveSchema));
        }

        /// <summary>
        /// Matches a primitive schema to its type name.
        /// </summary>
        protected virtual string GetSchemaToken(PrimitiveSchema schema)
        {
            switch (schema)
            {
                case BooleanSchema _:
                    return JsonSchemaToken.Boolean;

                case BytesSchema _:
                    return JsonSchemaToken.Bytes;

                case DoubleSchema _:
                    return JsonSchemaToken.Double;

                case FloatSchema _:
                    return JsonSchemaToken.Float;

                case IntSchema _:
                    return JsonSchemaToken.Int;

                case LongSchema _:
                    return JsonSchemaToken.Long;

                case NullSchema _:
                    return JsonSchemaToken.Null;

                case StringSchema _:
                    return JsonSchemaToken.String;

                default:
                    throw new UnsupportedSchemaException(schema, $"Unknown primitive schema {schema.GetType().Name}.");
            }
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="RecordSchema" />.
    /// </summary>
    public class RecordJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// A schema writer to use to write field schemas.
        /// </summary>
        protected readonly IJsonSchemaWriter Writer;

        /// <summary>
        /// Creates a new record case.
        /// </summary>
        /// <param name="writer">
        /// A schema writer to use to write field schemas.
        /// </param>
        public RecordJsonSchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is RecordSchema;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is RecordSchema recordSchema))
            {
                throw new ArgumentException("The record case can only be applied to a record schema.");
            }

            if (names.TryGetValue(recordSchema.FullName, out var existing))
            {
                if (!schema.Equals(existing))
                {
                    throw new InvalidSchemaException($"A conflicting schema with the name {recordSchema.FullName} has already been written.");
                }

                json.WriteStringValue(recordSchema.FullName);
                return;
            }

            if (!names.TryAdd(recordSchema.FullName, recordSchema))
            {
                throw new InvalidOperationException();
            }

            json.WriteStartObject();
            json.WriteString(JsonAttributeToken.Name, recordSchema.FullName);

            if (!canonical)
            {
                if (recordSchema.Aliases.Count > 0)
                {
                    json.WritePropertyName(JsonAttributeToken.Aliases);
                    json.WriteStartArray();

                    foreach (var alias in recordSchema.Aliases)
                    {
                        json.WriteStringValue(alias);
                    }

                    json.WriteEndArray();
                }

                if (!string.IsNullOrEmpty(recordSchema.Documentation))
                {
                    json.WriteString(JsonAttributeToken.Doc, recordSchema.Documentation);
                }
            }

            json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.Record);
            json.WritePropertyName(JsonAttributeToken.Fields);
            json.WriteStartArray();

            foreach (var field in recordSchema.Fields)
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Name, field.Name);

                if (!canonical && !string.IsNullOrEmpty(field.Documentation))
                {
                    json.WriteString(JsonAttributeToken.Doc, field.Documentation);
                }

                json.WritePropertyName(JsonAttributeToken.Type);
                Writer.Write(field.Type, json, canonical, names);
                json.WriteEndObject();
            }

            json.WriteEndArray();
            json.WriteEndObject();
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="UnionSchema" />.
    /// </summary>
    public class UnionJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// A schema writer to use to write child schemas.
        /// </summary>
        protected readonly IJsonSchemaWriter Writer;

        /// <summary>
        /// Creates a new union case.
        /// </summary>
        /// <param name="writer">
        /// A schema writer to use to write child schemas.
        /// </param>
        public UnionJsonSchemaWriterCase(IJsonSchemaWriter writer)
        {
            Writer = writer ?? throw new ArgumentNullException(nameof(writer), "Schema writer cannot be null.");
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is UnionSchema;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!(schema is UnionSchema unionSchema))
            {
                throw new ArgumentException("The union case can only be applied to a union schema.");
            }

            json.WriteStartArray();

            foreach (var child in unionSchema.Schemas)
            {
                Writer.Write(child, json, canonical, names);
            }

            json.WriteEndArray();
        }
    }

    /// <summary>
    /// A JSON schema writer case that matches <see cref="UuidLogicalType" />.
    /// </summary>
    public class UuidJsonSchemaWriterCase : JsonSchemaWriterCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public override bool IsMatch(Schema schema)
        {
            return schema is StringSchema && schema.LogicalType is UuidLogicalType;
        }

        /// <summary>
        /// Writes a schema to JSON.
        /// </summary>
        /// <param name="schema">
        /// The schema to write.
        /// </param>
        /// <param name="json">
        /// The JSON writer to use for output.
        /// </param>
        /// <param name="canonical">
        /// Whether the schema should be written in Parsing Canonical Form (i.e., built without
        /// nonessential attributes).
        /// </param>
        /// <param name="names">
        /// A schema cache. The cache is populated as the schema is written and can be used to
        /// determine which named schemas have already been processed.
        /// </param>
        public override void Write(Schema schema, Utf8JsonWriter json, bool canonical, ConcurrentDictionary<string, NamedSchema> names)
        {
            if (!IsMatch(schema))
            {
                throw new ArgumentException("The UUID case can only be applied to a string schema with a UUID logical type.");
            }

            if (canonical)
            {
                json.WriteStringValue(JsonSchemaToken.String);
            }
            else
            {
                json.WriteStartObject();
                json.WriteString(JsonAttributeToken.Type, JsonSchemaToken.String);
                json.WriteString(JsonAttributeToken.LogicalType, JsonSchemaToken.Uuid);
                json.WriteEndObject();
            }
        }
    }
}
