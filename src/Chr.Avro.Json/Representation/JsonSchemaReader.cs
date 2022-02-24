namespace Chr.Avro.Representation
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    /// <summary>
    /// Reads JSON-serialized Avro schemas.
    /// </summary>
    public class JsonSchemaReader : IJsonSchemaReader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSchemaReader" /> class configured with
        /// the default list of cases.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance to use when deserializing default values to .NET
        /// objects. If none is provided, the default <see cref="JsonDeserializerBuilder" />
        /// will be used.
        /// </param>
        public JsonSchemaReader(IJsonDeserializerBuilder? deserializerBuilder = default)
            : this(CreateDefaultCaseBuilders(deserializerBuilder))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSchemaReader" /> class configured with
        /// a custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        public JsonSchemaReader(IEnumerable<Func<IJsonSchemaReader, IJsonSchemaReaderCase>> caseBuilders)
        {
            var cases = new List<IJsonSchemaReaderCase>();

            Cases = cases;

            // initialize cases last so that the schema reader is fully ready:
            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Gets the list of cases that the schema reader will attempt to apply. If the first case
        /// does not match, the schema reader will try the next case, and so on until all cases
        /// have been tried.
        /// </summary>
        public IEnumerable<IJsonSchemaReaderCase> Cases { get; }

        /// <summary>
        /// Creates the default list of case builders.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance to use when deserializing deserializing default values
        /// to .NET objects. If none is provided, the default <see cref="JsonDeserializerBuilder" />
        /// will be used.
        /// </param>
        /// <returns>
        /// A list of case builders that matches all types defined in the Avro spec.
        /// </returns>
        public static IEnumerable<Func<IJsonSchemaReader, IJsonSchemaReaderCase>> CreateDefaultCaseBuilders(IJsonDeserializerBuilder? deserializerBuilder = default)
        {
            deserializerBuilder ??= new JsonDeserializerBuilder();

            return new Func<IJsonSchemaReader, IJsonSchemaReaderCase>[]
            {
                // logical types:
                reader => new JsonDateSchemaReaderCase(),
                reader => new JsonDecimalSchemaReaderCase(),
                reader => new JsonDurationSchemaReaderCase(),
                reader => new JsonMicrosecondTimeSchemaReaderCase(),
                reader => new JsonMicrosecondTimestampSchemaReaderCase(),
                reader => new JsonMillisecondTimeSchemaReaderCase(),
                reader => new JsonMillisecondTimestampSchemaReaderCase(),
                reader => new JsonUuidSchemaReaderCase(),

                // collections:
                reader => new JsonArraySchemaReaderCase(reader),
                reader => new JsonMapSchemaReaderCase(reader),

                // unions:
                reader => new JsonUnionSchemaReaderCase(reader),

                // named:
                reader => new JsonEnumSchemaReaderCase(),
                reader => new JsonFixedSchemaReaderCase(),
                reader => new JsonRecordSchemaReaderCase(deserializerBuilder, reader),

                // others:
                reader => new JsonPrimitiveSchemaReaderCase(),
                reader => new JsonNamedSchemaReaderCase(),
            };
        }

        /// <exception cref="UnknownSchemaException">
        /// Thrown when no case matches the schema read from <paramref name="stream" /> or when a
        /// matching case fails.
        /// </exception>
        /// <inheritdoc />
        public Schema Read(Stream stream, JsonSchemaReaderContext? context = default)
        {
            using var document = JsonDocument.Parse(stream);
            return Read(document.RootElement, context);
        }

        /// <exception cref="UnknownSchemaException">
        /// Thrown when no case matches <paramref name="schema" /> or when a matching case fails.
        /// </exception>
        /// <inheritdoc />
        public Schema Read(string schema, JsonSchemaReaderContext? context = default)
        {
            using var document = JsonDocument.Parse(schema);
            return Read(document.RootElement, context);
        }

        /// <exception cref="UnknownSchemaException">
        /// Thrown when no case matches <paramref name="element" /> or when a matching case fails.
        /// </exception>
        /// <inheritdoc />
        public Schema Read(JsonElement element, JsonSchemaReaderContext? context = default)
        {
            context ??= new JsonSchemaReaderContext();

            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.Read(element, context);

                if (result.Schema != null)
                {
                    return result.Schema;
                }

                exceptions.AddRange(result.Exceptions);
            }

            throw new UnknownSchemaException($"No schema reader case matched {element}.", new AggregateException(exceptions));
        }
    }
}
