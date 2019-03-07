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
    /// An <see cref="IAsyncDeserializer{T}" /> that resolves schemas on the fly. When deserializing
    /// messages, this deserializer will attempt to derive a schema ID from the first five bytes.
    /// (For more information, see the <a href="https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format">Confluent wire format documentation</a>.)
    /// If a schema with that ID is not found in cache, it will attempt to pull down a matching
    /// schema from the Schema Registry.
    /// </summary>
    public class AsyncSchemaRegistryDeserializer<T> : IAsyncDeserializer<T>
    {
        private readonly IBinaryDeserializerBuilder _builder;

        private readonly ConcurrentDictionary<int, Task<Func<Stream, T>>> _cache;

        private readonly IJsonSchemaReader _reader;

        private readonly ISchemaRegistryClient _registry;

        /// <summary>
        /// Creates a deserializer.
        /// </summary>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="builder">
        /// A deserializer builder (used to build deserialization functions for C# types). If none
        /// is provided, the default deserializer builder will be used.
        /// </param>
        /// <param name="reader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        public AsyncSchemaRegistryDeserializer(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            IBinaryDeserializerBuilder builder = null,
            IJsonSchemaReader reader = null
        ) : this(new CachedSchemaRegistryClient(registryConfiguration), builder, reader) { }

        /// <summary>
        /// Creates a deserializer.
        /// </summary>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <param name="builder">
        /// A deserializer builder (used to build deserialization functions for C# types). If none
        /// is provided, the default deserializer builder will be used.
        /// </param>
        /// <param name="reader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public AsyncSchemaRegistryDeserializer(
            ISchemaRegistryClient registryClient,
            IBinaryDeserializerBuilder builder = null,
            IJsonSchemaReader reader = null
        ) {
            _builder = builder ?? new BinaryDeserializerBuilder();
            _cache = new ConcurrentDictionary<int, Task<Func<Stream, T>>>();
            _reader = reader ?? new JsonSchemaReader();
            _registry = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
        }

        /// <summary>
        /// Deserialize an incoming message. (See <see cref="IAsyncDeserializer{T}.DeserializeAsync(ReadOnlyMemory{byte}, bool, bool, MessageMetadata, TopicPartition)" />.)
        /// </summary>
        public async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, bool isKey, MessageMetadata messageMetadata, TopicPartition source)
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

                var deserialize = await (_cache.GetOrAdd(BitConverter.ToInt32(bytes, 0), async id =>
                    _builder.BuildDelegate<T>(_reader.Read(await _registry.GetSchemaAsync(id)))
                ));

                return deserialize(stream);
            }
        }
    }
}
