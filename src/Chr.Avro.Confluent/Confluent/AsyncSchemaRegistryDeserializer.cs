namespace Chr.Avro.Confluent
{
    using System;
    using System.Buffers.Binary;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Chr.Avro.Abstract;
    using Chr.Avro.Representation;
    using Chr.Avro.Serialization;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;

#if NET8_0_OR_GREATER
    using System.Collections.Frozen;
#endif

    /// <summary>
    /// An <see cref="IAsyncDeserializer{T}" /> that resolves Avro schemas on the fly. When
    /// deserializing messages, this deserializer will attempt to derive a schema ID from the first
    /// five bytes. (For more information, see the <a href="https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format">Confluent wire format documentation</a>.)
    /// If a schema with that ID is not found in cache, it will attempt to pull down a matching
    /// schema from the Schema Registry.
    /// </summary>
    /// <inheritdoc />
    public class AsyncSchemaRegistryDeserializer<T> : IAsyncDeserializer<T>, IDisposable
    {
#if NET8_0_OR_GREATER
        private FrozenDictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>> cache = FrozenDictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>>.Empty;
#else
        private Dictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>> cache = new Dictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>>();
#endif
        private readonly object cacheLock = new object();
        private readonly bool disposeRegistryClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncSchemaRegistryDeserializer{T}" />
        /// class with a Schema Registry configuration.
        /// </summary>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that should be used to generate deserialization
        /// functions for .NET <see cref="Type" />s. If none is provided, the default
        /// <see cref="BinaryDeserializerBuilder" /> will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A schema reader instance that should be used to convert schemas received from the
        /// Registry into abstract representations. If none is provided, the default
        /// <see cref="JsonSchemaReader" /> will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="registryConfiguration" /> is <c>null</c>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="tombstoneBehavior" /> is incompatible with
        /// <typeparamref name="T" />.
        /// </exception>
        public AsyncSchemaRegistryDeserializer(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
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

            disposeRegistryClient = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncSchemaRegistryDeserializer{T}" />
        /// class with a Schema Registry client.
        /// </summary>
        /// <param name="registryClient">
        /// A Schema Registry client to use for Registry operations. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that should be used to generate deserialization
        /// functions for .NET <see cref="Type" />s. If none is provided, the default
        /// <see cref="BinaryDeserializerBuilder" /> will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A schema reader instance that should be used to convert schemas received from the
        /// Registry into abstract representations. If none is provided, the default
        /// <see cref="JsonSchemaReader" /> will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="registryClient" /> is <c>null</c>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="tombstoneBehavior" /> is incompatible with
        /// <typeparamref name="T" />.
        /// </exception>
        public AsyncSchemaRegistryDeserializer(
            ISchemaRegistryClient registryClient,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            if (tombstoneBehavior != TombstoneBehavior.None && default(T) != null)
            {
                throw new UnsupportedTypeException(typeof(T), $"{typeof(T)} cannot represent tombstone values.");
            }

            DeserializerBuilder = deserializerBuilder ?? new BinaryDeserializerBuilder();
            RegistryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
            SchemaReader = schemaReader ?? new JsonSchemaReader();
            TombstoneBehavior = tombstoneBehavior;

            disposeRegistryClient = false;
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to generate deserialization
        /// functions.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Gets the Schema Registry client that will be used for Registry operations.
        /// </summary>
        public ISchemaRegistryClient RegistryClient { get; }

        /// <summary>
        /// Gets the JSON schema reader instance that will be used to convert schemas received from
        /// the Registry into abstract representations.
        /// </summary>
        public IJsonSchemaReader SchemaReader { get; }

        /// <summary>
        /// Gets an value describing how the deserializer will handle tombstone records.
        /// </summary>
        public TombstoneBehavior TombstoneBehavior { get; }

        /// <inheritdoc />
        Task<T> IAsyncDeserializer<T>.DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            return DeserializeAsync(data, isNull, context).AsTask();
        }

        /// <inheritdoc cref="IAsyncDeserializer{T}.DeserializeAsync" />
        public virtual async ValueTask<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull && TombstoneBehavior != TombstoneBehavior.None)
            {
                if (context.Component == MessageComponentType.Value || TombstoneBehavior != TombstoneBehavior.Strict)
                {
                    return default;
                }
            }

            var id = DeserializeSchemaId(data);
            Task<Func<ReadOnlyMemory<byte>, T>> task;

            if (!Volatile.Read(ref cache)!.TryGetValue(id, out task) || task.IsCanceled || task.IsFaulted)
            {
                lock (cacheLock)
                {
                    if (!cache.TryGetValue(id, out task) || task.IsCanceled || task.IsFaulted)
                    {
                        var clone = new Dictionary<int, Task<Func<ReadOnlyMemory<byte>, T>>>(cache)
                        {
                            [id] = task = ((Func<int, Task<Func<ReadOnlyMemory<byte>, T>>>)(async id =>
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
                            }))(id),
                        };
#if NET8_0_OR_GREATER
                        Volatile.Write(ref cache, clone.ToFrozenDictionary());
#else
                        Volatile.Write(ref cache, clone);
#endif
                    }
                }
            }

            return (await task.ConfigureAwait(false))(data);
        }

        private static int DeserializeSchemaId(ReadOnlyMemory<byte> data)
        {
            if (data.Length < 5)
            {
                throw new InvalidEncodingException(0, "The encoded data does not include a Confluent wire format header.");
            }

#if NET6_0_OR_GREATER
            var header = data.Span[..5];

            if (header[0] != 0x00)
            {
                throw new InvalidEncodingException(0, "The encoded data does not conform to the Confluent wire format.");
            }

            return BinaryPrimitives.ReadInt32BigEndian(header[1..]);
#else
            var header = data.Slice(0, 5).ToArray();

            if (header[0] != 0x00)
            {
                throw new InvalidEncodingException(0, "The encoded data does not conform to the Confluent wire format.");
            }

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(header, 1, 4);
            }

            return BitConverter.ToInt32(header, 1);
#endif
        }

        /// <summary>
        /// Disposes the deserializer, freeing up any resources.
        /// </summary>
        public void Dispose()
        {
            if (disposeRegistryClient)
            {
                RegistryClient.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Builds a deserialization function for the Confluent wire format.
        /// </summary>
        /// <param name="schema">
        /// The <see cref="Abstract.Schema" /> to build the Avro deserializer from.
        /// </param>
        /// <returns>
        /// A function configured to deserialize from a <see cref="ReadOnlyMemory{Byte}" />
        /// starting from position <c>5</c>. (Since the header is read by
        /// <see cref="DeserializeAsync(ReadOnlyMemory{byte}, bool, SerializationContext)" />, no
        /// validation needs to be performed.)
        /// </returns>
        protected virtual Func<ReadOnlyMemory<byte>, T> Build(Abstract.Schema schema)
        {
            // the reader, as a ref struct, can't be declared within an async or lambda function, so
            // build it into the delegate:
            var inner = DeserializerBuilder.BuildDelegateExpression<T>(schema);
            var memory = Expression.Parameter(typeof(ReadOnlyMemory<byte>));
            var span = Expression.Parameter(typeof(ReadOnlySpan<byte>));

            var getSpan = memory.Type
                .GetProperty(nameof(ReadOnlyMemory<byte>.Span))
                .GetGetMethod();

            var readerConstructor = inner.Parameters[0].Type
                .GetConstructor(new[] { span.Type });

            var slice = memory.Type
                .GetMethod(nameof(ReadOnlyMemory<byte>.Slice), new[] { typeof(int) });

            if (schema is BytesSchema)
            {
                inner = new WireFormatBytesDeserializerRewriter(span)
                    .VisitAndConvert(inner, GetType().Name);
            }

            var reader = Expression.Block(
                new[] { span },
                Expression.Assign(
                    span,
                    Expression.Property(
                        Expression.Call(memory, slice, Expression.Constant(5)),
                        getSpan)),
                Expression.Invoke(
                    inner,
                    Expression.New(readerConstructor, span)));

            return Expression.Lambda<Func<ReadOnlyMemory<byte>, T>>(reader, memory).Compile();
        }
    }
}
