using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
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
    public class AsyncSchemaRegistryDeserializer<T> : IAsyncDeserializer<T>, IDisposable
    {
        /// <summary>
        /// The builder used to generate deserialization functions.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// The client used for Schema Registry operations.
        /// </summary>
        public ISchemaRegistryClient RegistryClient { get; }

        /// <summary>
        /// The JSON schema reader used to convert schemas received from the registry into abstract
        /// representations.
        /// </summary>
        public IJsonSchemaReader SchemaReader { get; }

        /// <summary>
        /// The behavior of the deserializer on tombstone records.
        /// </summary>
        public TombstoneBehavior TombstoneBehavior { get; }

        private readonly IDictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>> _cache;

        private readonly bool _disposeRegistryClient;

        /// <summary>
        /// Creates a deserializer.
        /// </summary>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to to generate deserialization functions for C# types.
        /// If none is provided, the default deserializer builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// The JSON schema reader to use to convert schemas received from the registry into abstract
        /// representations. If none is provided, the default schema reader will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the deserializer on tombstone records.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry configuration is null.
        /// </exception>
        public AsyncSchemaRegistryDeserializer(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        ) {
            if (registryConfiguration == null)
            {
                throw new ArgumentNullException(nameof(registryConfiguration));
            }

            if (tombstoneBehavior != TombstoneBehavior.None && default(T) != null)
            {
                throw new UnsupportedTypeException(typeof(T), $"{typeof(T)} cannot represent tombstone values.");
            }

            DeserializerBuilder = deserializerBuilder ?? new BinaryDeserializerBuilder();
            RegistryClient = new CachedSchemaRegistryClient(registryConfiguration);
            SchemaReader = schemaReader ?? new JsonSchemaReader();
            TombstoneBehavior = tombstoneBehavior;

            _cache = new Dictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>>();
            _disposeRegistryClient = true;
        }

        /// <summary>
        /// Creates a deserializer.
        /// </summary>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <param name="deserializerBuilder">
        /// The deserializer builder used to generate deserialization functions for C# types. If
        /// none is provided, the default deserializer builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// The JSON schema reader used to convert schemas received from the registry into abstract
        /// representations. If none is provided, the default schema reader will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the deserializer on tombstone records.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public AsyncSchemaRegistryDeserializer(
            ISchemaRegistryClient registryClient,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        ) {
            if (registryClient == null)
            {
                throw new ArgumentNullException(nameof(registryClient));
            }

            if (tombstoneBehavior != TombstoneBehavior.None && default(T) != null)
            {
                throw new UnsupportedTypeException(typeof(T), $"{typeof(T)} cannot represent tombstone values.");
            }

            DeserializerBuilder = deserializerBuilder ?? new BinaryDeserializerBuilder();
            RegistryClient = registryClient;
            SchemaReader = schemaReader ?? new JsonSchemaReader();
            TombstoneBehavior = tombstoneBehavior;

            _cache = new Dictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>>();
            _disposeRegistryClient = false;
        }

        /// <summary>
        /// Deserialize a message. (See <see cref="IAsyncDeserializer{T}.DeserializeAsync(ReadOnlyMemory{byte}, bool, SerializationContext)" />.)
        /// </summary>
        public virtual async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull && TombstoneBehavior != TombstoneBehavior.None)
            {
                if (context.Component == MessageComponentType.Value || TombstoneBehavior != TombstoneBehavior.Strict)
                {
                    return default;
                }
            }

            if (data.Length < 5)
            {
                throw new InvalidDataException("The encoded data does not include a Confluent wire format header.");
            }

            var header = data.Slice(0, 5).ToArray();

            if (header[0] != 0x00)
            {
                throw new InvalidDataException("The encoded data does not conform to the Confluent wire format.");
            }

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(header, 1, 4);
            }

            var id = BitConverter.ToInt32(header, 1);

            Task<Func<ReadOnlyMemory<byte>, T>> task;

            lock (_cache)
            {
                if (!_cache.TryGetValue(id, out task) || task.IsFaulted)
                {
                    _cache[id] = task = ((Func<int, Task<Func<ReadOnlyMemory<byte>, T>>>)(async id =>
                    {
                        var registration = await RegistryClient.GetSchemaAsync(id).ConfigureAwait(false);

                        if (registration.SchemaType != SchemaType.Avro)
                        {
                            throw new UnsupportedSchemaException(null, $"The schema used to encode the data ({id}) is not an Avro schema.");
                        }

                        var schema = SchemaReader.Read(registration.SchemaString);

                        if (TombstoneBehavior != TombstoneBehavior.None)
                        {
                            var hasNull = schema is NullSchema
                                || (schema is UnionSchema union && union.Schemas.Any(s => s is NullSchema));

                            if (TombstoneBehavior == TombstoneBehavior.Strict && hasNull)
                            {
                                throw new UnsupportedSchemaException(schema, "Tombstone deserialization is not supported for schemas that can represent null values.");
                            }
                        }

                        return Build(schema);
                    }))(id);
                }
            }

            return (await task.ConfigureAwait(false))(data);
        }

        /// <summary>
        /// Builds a deserializer for the Confluent wire format.
        /// </summary>
        /// <param name="schema">
        /// The schema to build the Avro deserializer from.
        /// </param>
        protected virtual Func<ReadOnlyMemory<byte>, T> Build(Abstract.Schema schema)
        {
            // the reader, as a ref struct, can't be declared within an async or lambda function, so
            // build it into the delegate:
            var inner = DeserializerBuilder.BuildExpression<T>(schema);
            var memory = Expression.Parameter(typeof(ReadOnlyMemory<byte>));

            var getSpan = memory.Type
                .GetProperty(nameof(ReadOnlyMemory<byte>.Span))
                .GetGetMethod();

            var readerConstructor = inner.Parameters[0].Type
                .GetConstructor(new[] { getSpan.ReturnType });

            var slice = memory.Type
                .GetMethod(nameof(ReadOnlyMemory<byte>.Slice), new[] { typeof(int) });

            var reader = Expression.Invoke(inner,
                Expression.New(
                    readerConstructor,
                    Expression.Property(
                        Expression.Call(memory, slice, Expression.Constant(5)),
                        getSpan)));

            return Expression.Lambda<Func<ReadOnlyMemory<byte>, T>>(reader, true, memory).Compile();
        }

        /// <summary>
        /// Disposes the deserializer, freeing up any resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the deserializer, freeing up any resources.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_disposeRegistryClient)
                {
                    RegistryClient.Dispose();
                }
            }
        }
    }
}
