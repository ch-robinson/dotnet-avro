using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// An <see cref="IAsyncSerializer{T}" /> that resolves Avro schemas on the fly. When serializing
    /// messages, this serializer will attempt to look up a subject that matches the topic name (if
    /// not already cached).
    /// </summary>
    /// <remarks>
    /// By default, when serializing keys for a topic with name "test_topic", this deserializer
    /// would query the Schema Registry for subject "test_topic-key". (This is a Confluent
    /// convention—values would be "test_topic-value".)
    /// </remarks>
    public class AsyncSchemaRegistrySerializer<T> : IAsyncSerializer<T>
    {
        private readonly ConcurrentDictionary<string, Task<Func<T, byte[]>>> _cache;

        private readonly Func<string, string, Task<int>> _register;

        private readonly bool _registerAutomatically;

        private readonly bool _resolveReferenceTypesAsNullable;

        private readonly Func<string, Task<Schema>> _resolve;

        private readonly Abstract.ISchemaBuilder _schemaBuilder;

        private readonly IJsonSchemaReader _schemaReader;

        private readonly IJsonSchemaWriter _schemaWriter;

        private readonly IBinarySerializerBuilder _serializerBuilder;

        private readonly Func<SerializationContext, string> _subjectNameBuilder;

        /// <summary>
        /// Creates a serializer.
        /// </summary>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder (used to build a schema for a C# type when registering automatically).
        /// If none is provided, the default schema builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        /// <param name="schemaWriter">
        /// A JSON schema writer (used to convert abstract schema representations when registering
        /// automatically). If none is provided, the default schema writer will be used.
        /// </param>
        /// <param name="serializerBuilder">
        /// A deserializer builder (used to build serialization functions for C# types). If none is
        /// provided, the default serializer builder will be used.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry configuration is null.
        /// </exception>
        public AsyncSchemaRegistrySerializer(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) : this(
            registerAutomatically,
            resolveReferenceTypesAsNullable,
            schemaBuilder,
            schemaReader,
            schemaWriter,
            serializerBuilder,
            subjectNameBuilder
        ) {
            if (registryConfiguration == null)
            {
                throw new ArgumentNullException(nameof(registryConfiguration));
            }

            _register = async (subject, json) =>
            {
                using (var registry = new CachedSchemaRegistryClient(registryConfiguration))
                {
                    return await registry.RegisterSchemaAsync(subject, json).ConfigureAwait(false);
                }
            };

            _resolve = async subject =>
            {
                using (var registry = new CachedSchemaRegistryClient(registryConfiguration))
                {
                    return await registry.GetLatestSchemaAsync(subject).ConfigureAwait(false);
                }
            };
        }

        /// <summary>
        /// Creates a serializer.
        /// </summary>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder (used to build a schema for a C# type when registering automatically).
        /// If none is provided, the default schema builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        /// <param name="schemaWriter">
        /// A JSON schema writer (used to convert abstract schema representations when registering
        /// automatically). If none is provided, the default schema writer will be used.
        /// </param>
        /// <param name="serializerBuilder">
        /// A deserializer builder (used to build serialization functions for C# types). If none is
        /// provided, the default serializer builder will be used.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public AsyncSchemaRegistrySerializer(
            ISchemaRegistryClient registryClient,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) : this(
            registerAutomatically,
            resolveReferenceTypesAsNullable,
            schemaBuilder,
            schemaReader,
            schemaWriter,
            serializerBuilder,
            subjectNameBuilder
        ) {
            if (registryClient == null)
            {
                throw new ArgumentNullException(nameof(registryClient));
            }

            _register = (subject, json) => registryClient.RegisterSchemaAsync(subject, json);
            _resolve = subject => registryClient.GetLatestSchemaAsync(subject);
        }

        private AsyncSchemaRegistrySerializer(
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) {
            _cache = new ConcurrentDictionary<string, Task<Func<T, byte[]>>>();
            _registerAutomatically = registerAutomatically;
            _resolveReferenceTypesAsNullable = resolveReferenceTypesAsNullable;
            _schemaBuilder = schemaBuilder ?? new Abstract.SchemaBuilder();
            _schemaReader = schemaReader ?? new JsonSchemaReader();
            _schemaWriter = schemaWriter ?? new JsonSchemaWriter();
            _serializerBuilder = serializerBuilder ?? new BinarySerializerBuilder(resolveReferenceTypesAsNullable: _resolveReferenceTypesAsNullable);
            _subjectNameBuilder = subjectNameBuilder ??
                (c => $"{c.Topic}-{(c.Component == MessageComponentType.Key ? "key" : "value")}");
        }

        /// <summary>
        /// Serialize a message. (See <see cref="IAsyncSerializer{T}.SerializeAsync(T, SerializationContext)" />.)
        /// </summary>
        public virtual async Task<byte[]> SerializeAsync(T data, SerializationContext context)
        {
            var serialize = await (_cache.GetOrAdd(_subjectNameBuilder(context), async subject =>
            {
                int id;
                Action<T, Stream> @delegate;

                try
                {
                    var existing = await _resolve(subject).ConfigureAwait(false);
                    var schema = _schemaReader.Read(existing.SchemaString);

                    @delegate = _serializerBuilder.BuildDelegate<T>(schema);
                    id = existing.Id;
                }
                catch (Exception e) when (_registerAutomatically && (
                    (e is SchemaRegistryException sre && sre.ErrorCode == 40401) ||
                    (e is UnsupportedTypeException)
                ))
                {
                    var schema = _schemaBuilder.BuildSchema<T>();
                    var json = _schemaWriter.Write(schema);

                    @delegate = _serializerBuilder.BuildDelegate<T>(schema);
                    id = await _register(subject, json).ConfigureAwait(false);
                }

                var bytes = BitConverter.GetBytes(id);

                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(bytes);
                }

                return value =>
                {
                    var stream = new MemoryStream();

                    using (stream)
                    {
                        stream.WriteByte(0x00);
                        stream.Write(bytes, 0, bytes.Length);

                        @delegate(value, stream);
                    }

                    return stream.ToArray();
                };
            })).ConfigureAwait(false);

            return serialize(data);
        }
    }
}
