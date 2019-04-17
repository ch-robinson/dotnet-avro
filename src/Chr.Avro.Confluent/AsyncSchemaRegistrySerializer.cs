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
    /// An <see cref="IAsyncSerializer{T}" /> that resolves schemas on the fly. When serializing
    /// messages, this serializer will attempt to look up a schema that matches the topic (if one
    /// isn’t already cached).
    /// </summary>
    /// <remarks>
    /// By default, when serializing keys for a topic with name "test_topic", this deserializer
    /// would query the Schema Registry for subject "test_topic-key". (This is a Confluent
    /// convention—values would be "test_topic-value".)
    /// </remarks>
    public class AsyncSchemaRegistrySerializer<T> : IAsyncSerializer<T>
    {
        private readonly ConcurrentDictionary<string, Task<Func<T, byte[]>>> _cache;

        private readonly bool _registerAutomatically;

        private readonly ISchemaRegistryClient _registryClient;

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
        public AsyncSchemaRegistrySerializer(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            bool registerAutomatically = false,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) : this(
            new CachedSchemaRegistryClient(registryConfiguration),
            registerAutomatically,
            schemaBuilder,
            schemaReader,
            schemaWriter,
            serializerBuilder,
            subjectNameBuilder
        ) { }

        /// <summary>
        /// Creates a serializer.
        /// </summary>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
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
        public AsyncSchemaRegistrySerializer(
            ISchemaRegistryClient registryClient,
            bool registerAutomatically = false,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) {
            _cache = new ConcurrentDictionary<string, Task<Func<T, byte[]>>>();
            _registerAutomatically = registerAutomatically;
            _registryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
            _schemaBuilder = schemaBuilder ?? new Abstract.SchemaBuilder();
            _schemaReader = schemaReader ?? new JsonSchemaReader();
            _schemaWriter = schemaWriter ?? new JsonSchemaWriter();
            _serializerBuilder = serializerBuilder ?? new BinarySerializerBuilder();
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
                    var existing = await _registryClient.GetLatestSchemaAsync(subject);
                    var schema = _schemaReader.Read(existing.SchemaString);

                    id = existing.Id;
                    @delegate = _serializerBuilder.BuildDelegate<T>(schema);
                }
                catch (Exception e) when (_registerAutomatically && (
                    (e is SchemaRegistryException sre && sre.ErrorCode == 40401) ||
                    (e is UnsupportedTypeException)
                )) {
                    var schema = _schemaBuilder.BuildSchema<T>();

                    @delegate = _serializerBuilder.BuildDelegate<T>(schema);
                    id = await _registryClient.RegisterSchemaAsync(subject, _schemaWriter.Write(schema));
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
            }));

            return serialize(data);
        }
    }
}
