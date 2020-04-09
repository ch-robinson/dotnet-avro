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
    /// An <see cref="IAsyncDeserializer{T}" /> that resolves Avro schemas on the fly. When
    /// deserializing messages, this deserializer will attempt to derive a schema ID from the first
    /// five bytes. (For more information, see the <a href="https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format">Confluent wire format documentation</a>.)
    /// If a schema with that ID is not found in cache, it will attempt to pull down a matching
    /// schema from the Schema Registry.
    /// </summary>
    public class AsyncSchemaRegistryDeserializer<T> : IAsyncDeserializer<T>
    {
        private readonly IDictionary<int, Task<Func<Stream, T>>> _cache;

        private readonly IBinaryDeserializerBuilder _deserializerBuilder;

        private readonly Func<int, Task<string>> _resolve;

        private readonly IJsonSchemaReader _schemaReader;

        /// <summary>
        /// Creates a deserializer.
        /// </summary>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder (used to build deserialization functions for C# types). If none
        /// is provided, the default deserializer builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry configuration is null.
        /// </exception>
        public AsyncSchemaRegistryDeserializer(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null
        ) : this(
            deserializerBuilder,
            schemaReader
        ) {
            if (registryConfiguration == null)
            {
                throw new ArgumentNullException(nameof(registryConfiguration));
            }

            _resolve = async id =>
            {
                using (var registry = new CachedSchemaRegistryClient(registryConfiguration))
                {
                    return await registry.GetSchemaAsync(id).ConfigureAwait(false);
                }
            };
        }

        /// <summary>
        /// Creates a deserializer.
        /// </summary>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder (used to build deserialization functions for C# types). If none
        /// is provided, the default deserializer builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public AsyncSchemaRegistryDeserializer(
            ISchemaRegistryClient registryClient,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null
        ) : this(
            deserializerBuilder,
            schemaReader
        ) {
            if (registryClient == null)
            {
                throw new ArgumentNullException(nameof(registryClient));
            }

            _resolve = id => registryClient.GetSchemaAsync(id);
        }

        private AsyncSchemaRegistryDeserializer(
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null
        ) {
            _cache = new Dictionary<int, Task<Func<Stream, T>>>();
            _deserializerBuilder = deserializerBuilder ?? new BinaryDeserializerBuilder();
            _schemaReader = schemaReader ?? new JsonSchemaReader();
        }

        /// <summary>
        /// Deserialize a message. (See <see cref="IAsyncDeserializer{T}.DeserializeAsync(ReadOnlyMemory{byte}, bool, SerializationContext)" />.)
        /// </summary>
        public virtual async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            using (var stream = new MemoryStream(data.ToArray(), false))
            {
                var bytes = new byte[4];

                if (stream.ReadByte() != 0x00 || stream.Read(bytes, 0, bytes.Length) != bytes.Length)
                {
                    throw new InvalidDataException("Data does not conform to the Confluent wire format.");
                }

                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(bytes);
                }

                var id = BitConverter.ToInt32(bytes, 0);

                Task<Func<Stream, T>> task;

                lock (_cache)
                {
                    if (!_cache.TryGetValue(id, out task) || task.IsFaulted)
                    {
                        _cache[id] = task = ((Func<int, Task<Func<Stream, T>>>)(async key =>
                        {
                            var json = await _resolve(key).ConfigureAwait(false);
                            var schema = _schemaReader.Read(json);

                            return _deserializerBuilder.BuildDelegate<T>(schema);
                        }))(id);
                    }
                }

                var deserialize = await task.ConfigureAwait(false);

                return deserialize(stream);
            }
        }
    }
}
