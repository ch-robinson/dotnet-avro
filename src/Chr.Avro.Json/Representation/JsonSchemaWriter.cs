namespace Chr.Avro.Representation
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Writes JSON-serialized Avro schemas.
    /// </summary>
    public class JsonSchemaWriter : IJsonSchemaWriter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSchemaWriter" /> class configured with
        /// the default list of cases.
        /// </summary>
        public JsonSchemaWriter()
            : this(CreateDefaultCaseBuilders())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSchemaWriter" /> class configured with
        /// a custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        public JsonSchemaWriter(IEnumerable<Func<IJsonSchemaWriter, IJsonSchemaWriterCase>> caseBuilders)
        {
            var cases = new List<IJsonSchemaWriterCase>();

            Cases = cases;

            // initialize cases last so that the schema writer is fully ready:
            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Gets the list of cases that the schema writer will attempt to apply. If the first case
        /// does not match, the schema writer will try the next case, and so on until all cases
        /// have been tried.
        /// </summary>
        public IEnumerable<IJsonSchemaWriterCase> Cases { get; }

        /// <summary>
        /// Creates the default list of case builders.
        /// </summary>
        /// <returns>
        /// A list of case builders that matches all abstract schema types.
        /// </returns>
        public static IEnumerable<Func<IJsonSchemaWriter, IJsonSchemaWriterCase>> CreateDefaultCaseBuilders()
        {
            return new Func<IJsonSchemaWriter, IJsonSchemaWriterCase>[]
            {
                // logical types:
                writer => new JsonDateSchemaWriterCase(),
                writer => new JsonDecimalSchemaWriterCase(),
                writer => new JsonDurationSchemaWriterCase(),
                writer => new JsonMicrosecondTimeSchemaWriterCase(),
                writer => new JsonMicrosecondTimestampSchemaWriterCase(),
                writer => new JsonMillisecondTimeSchemaWriterCase(),
                writer => new JsonMillisecondTimestampSchemaWriterCase(),
                writer => new JsonUuidSchemaWriterCase(),

                // collections:
                writer => new JsonArraySchemaWriterCase(writer),
                writer => new JsonMapSchemaWriterCase(writer),

                // unions:
                writer => new JsonUnionSchemaWriterCase(writer),

                // named:
                writer => new JsonEnumSchemaWriterCase(),
                writer => new JsonFixedSchemaWriterCase(),
                writer => new JsonRecordSchemaWriterCase(writer),

                // primitives:
                writer => new JsonPrimitiveSchemaWriterCase(),
            };
        }

        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when no matching case is found for <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual void Write(Schema schema, Stream stream, bool canonical = false, JsonSchemaWriterContext? context = default)
        {
            using var json = new Utf8JsonWriter(stream);

            Write(schema, json, canonical, context);
        }

        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when no matching case is found for <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual string Write(Schema schema, bool canonical = false, JsonSchemaWriterContext? context = default)
        {
            using var stream = new MemoryStream();
            using var reader = new StreamReader(stream);

            Write(schema, stream, canonical, context);

            stream.Seek(0, SeekOrigin.Begin);
            return reader.ReadToEnd();
        }

        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when no matching case is found for <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual void Write(Schema schema, Utf8JsonWriter json, bool canonical = false, JsonSchemaWriterContext? context = default)
        {
            context ??= new JsonSchemaWriterContext();

            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.Write(schema, json, canonical, context);

                if (result.Exceptions.Count > 0)
                {
                    exceptions.AddRange(result.Exceptions);
                }
                else
                {
                    return;
                }
            }

            throw new UnsupportedSchemaException(schema, $"No schema writer case matched {schema}.", new AggregateException(exceptions));
        }
    }
}
