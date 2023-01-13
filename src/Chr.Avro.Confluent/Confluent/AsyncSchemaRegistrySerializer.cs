namespace Chr.Avro.Confluent
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Chr.Avro.Representation;
    using Chr.Avro.Serialization;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;

    using BytesSchema = Chr.Avro.Abstract.BytesSchema;

    /// <summary>
    /// An <see cref="IAsyncSerializer{T}" /> that resolves Avro schemas on the fly. When serializing
    /// messages, this serializer will attempt to look up a subject that matches the topic name (if
    /// not already cached).
    /// </summary>
    /// <remarks>
    /// By default, when serializing keys for a topic with name <c>test_topic</c>, this deserializer
    /// would query the Schema Registry for subject <c>test_topic-key</c>. (This is a Confluent
    /// convention; the value subject would be <c>test_topic-value</c>.)
    /// </remarks>
    /// <inheritdoc />
    public class AsyncSchemaRegistrySerializer<T> : IAsyncSerializer<T>, IDisposable
    {
        private readonly IDictionary<string, Task<Func<T, byte[]>>> cache;

        private readonly bool disposeRegistryClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncSchemaRegistrySerializer{T}" />
        /// class with a Schema Registry configuration.
        /// </summary>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match the type being
        /// serialized.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that should be used to create schemas for .NET <see cref="Type" />s
        /// when registering automatically. If none is provided, the default <see cref="SchemaBuilder" />
        /// will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A schema reader instance that should be used to convert schemas received from the
        /// Registry into abstract representations. If none is provided, the default
        /// <see cref="JsonSchemaReader" /> will be used.
        /// </param>
        /// <param name="schemaWriter">
        /// A schema writer instance that should be used to convert abstract schema representations
        /// when registering automatically. If none is provided, the default <see cref="JsonSchemaWriter" />
        /// will be used.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that should be used to generate serialization functions
        /// for .NET <see cref="Type" />s. If none is provided, the default <see cref="BinarySerializerBuilder" />
        /// will be used.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given the topic name and a component type
        /// (key or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="registryConfiguration" /> is <c>null</c>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="tombstoneBehavior" /> is incompatible with
        /// <typeparamref name="T" />.
        /// </exception>
        public AsyncSchemaRegistrySerializer(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null,
            Func<SerializationContext, string> subjectNameBuilder = null,
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

            RegisterAutomatically = registerAutomatically;
            RegistryClient = new CachedSchemaRegistryClient(registryConfiguration);
            SchemaBuilder = schemaBuilder ?? new Abstract.SchemaBuilder();
            SchemaReader = schemaReader ?? new JsonSchemaReader();
            SchemaWriter = schemaWriter ?? new JsonSchemaWriter();
            SerializerBuilder = serializerBuilder ?? new BinarySerializerBuilder();
            SubjectNameBuilder = subjectNameBuilder ??
                (c => $"{c.Topic}-{(c.Component == MessageComponentType.Key ? "key" : "value")}");
            TombstoneBehavior = tombstoneBehavior;

            cache = new Dictionary<string, Task<Func<T, byte[]>>>();
            disposeRegistryClient = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncSchemaRegistrySerializer{T}" />
        /// class with a Schema Registry client.
        /// </summary>
        /// <param name="registryClient">
        /// A Schema Registry client to use for Registry operations. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match the type being
        /// serialized.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that should be used to create a schema for <typeparamref name="T" />
        /// when registering automatically. If none is provided, the default <see cref="SchemaBuilder" />
        /// will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A schema reader instance that should be used to convert schemas received from the
        /// Registry into abstract representations. If none is provided, the default
        /// <see cref="JsonSchemaReader" /> will be used.
        /// </param>
        /// <param name="schemaWriter">
        /// A schema writer instance that should be used to convert abstract schema representations
        /// when registering automatically. If none is provided, the default <see cref="JsonSchemaWriter" />
        /// will be used.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that should be used to generate serialization functions
        /// for .NET <see cref="Type" />s. If none is provided, the default <see cref="BinarySerializerBuilder" />
        /// will be used.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="registryClient" /> is <c>null</c>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="tombstoneBehavior" /> is incompatible with
        /// <typeparamref name="T" />.
        /// </exception>
        public AsyncSchemaRegistrySerializer(
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null,
            Func<SerializationContext, string> subjectNameBuilder = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            if (tombstoneBehavior != TombstoneBehavior.None && default(T) != null)
            {
                throw new UnsupportedTypeException(typeof(T), $"{typeof(T)} cannot represent tombstone values.");
            }

            RegisterAutomatically = registerAutomatically;
            RegistryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
            SchemaBuilder = schemaBuilder ?? new Abstract.SchemaBuilder();
            SchemaReader = schemaReader ?? new JsonSchemaReader();
            SchemaWriter = schemaWriter ?? new JsonSchemaWriter();
            SerializerBuilder = serializerBuilder ?? new BinarySerializerBuilder();
            SubjectNameBuilder = subjectNameBuilder ??
                (c => $"{c.Topic}-{(c.Component == MessageComponentType.Key ? "key" : "value")}");
            TombstoneBehavior = tombstoneBehavior;

            cache = new Dictionary<string, Task<Func<T, byte[]>>>();
            disposeRegistryClient = false;
        }

        /// <summary>
        /// Gets a value describing whether the serializer will automatically register a schema
        /// that matches <typeparamref name="T" />.
        /// </summary>
        public AutomaticRegistrationBehavior RegisterAutomatically { get; }

        /// <summary>
        /// Gets the Schema Registry client that will be used for Registry operations.
        /// </summary>
        public ISchemaRegistryClient RegistryClient { get; }

        /// <summary>
        /// Gets the schema builder instance that will be used to create a <see cref="Schema" />
        /// for <typeparamref name="T" /> when registering automatically.
        /// </summary>
        public Abstract.ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Gets the schema reader instance that will be used to convert schemas received from the
        /// Registry into abstract representations.
        /// </summary>
        public IJsonSchemaReader SchemaReader { get; }

        /// <summary>
        /// Gets the schema writer instance that will be used to convert abstract schema
        /// representations when registering automatically.
        /// </summary>
        public IJsonSchemaWriter SchemaWriter { get; }

        /// <summary>
        /// Gets the serializer builder instance that will be used to generate serialization
        /// functions.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Gets a function that determines a subject name given a topic name and a component type
        /// (key or value).
        /// </summary>
        public Func<SerializationContext, string> SubjectNameBuilder { get; }

        /// <summary>
        /// Gets an value describing how the serializer will handle tombstone records.
        /// </summary>
        public TombstoneBehavior TombstoneBehavior { get; }

        /// <summary>
        /// Disposes the serializer, freeing up any resources.
        /// </summary>
        public void Dispose()
        {
            if (disposeRegistryClient)
            {
                RegistryClient.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public virtual async Task<byte[]> SerializeAsync(T data, SerializationContext context)
        {
            var subject = SubjectNameBuilder(context);

            Task<Func<T, byte[]>> task;

            lock (cache)
            {
                if (!cache.TryGetValue(subject, out task) || task.IsCanceled || task.IsFaulted)
                {
                    cache[subject] = task = ((Func<string, Task<Func<T, byte[]>>>)(async subject =>
                    {
                        switch (RegisterAutomatically)
                        {
                            case AutomaticRegistrationBehavior.Always:
                                var schema = SchemaBuilder.BuildSchema<T>();
                                var id = await RegistryClient.RegisterSchemaAsync(subject, new Schema(SchemaWriter.Write(schema), SchemaType.Avro)).ConfigureAwait(false);

                                return Build(id, schema);

                            case AutomaticRegistrationBehavior.Never:
                                var registration = await RegistryClient.GetLatestSchemaAsync(subject).ConfigureAwait(false);

                                if (registration.SchemaType != SchemaType.Avro)
                                {
                                    throw new UnsupportedSchemaException(null, $"The latest schema with subject {subject} is not an Avro schema.");
                                }

                                return Build(registration.Id, SchemaReader.Read(registration.SchemaString));

                            default:
                                throw new ArgumentOutOfRangeException(nameof(RegisterAutomatically));
                        }
                    }))(subject);
                }
            }

            var serialize = await task.ConfigureAwait(false);

            if (data == null && TombstoneBehavior != TombstoneBehavior.None)
            {
                if (context.Component == MessageComponentType.Value || TombstoneBehavior != TombstoneBehavior.Strict)
                {
                    return null;
                }
            }

            return serialize(data);
        }

        /// <summary>
        /// Builds a serialization function for the Confluent wire format.
        /// </summary>
        /// <param name="id">
        /// A schema ID to include in each serialized payload.
        /// </param>
        /// <param name="schema">
        /// The schema to build the Avro serializer from.
        /// </param>
        /// <returns>
        /// A function configured to serialize <typeparamref name="T" /> to an array of bytes.
        /// </returns>
        protected virtual Func<T, byte[]> Build(int id, Abstract.Schema schema)
        {
            var header = new byte[5];
            Array.Copy(BitConverter.GetBytes(id), 0, header, 1, 4);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(header, 1, 4);
            }

            var inner = SerializerBuilder.BuildDelegateExpression<T>(schema);
            var stream = Expression.Parameter(typeof(MemoryStream));
            var value = inner.Parameters[0];

            var streamConstructor = typeof(MemoryStream)
                .GetConstructor(Type.EmptyTypes);

            var writerConstructor = inner.Parameters[1].Type
                .GetConstructor(new[] { typeof(Stream) });

            var dispose = typeof(IDisposable)
                .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

            var toArray = typeof(MemoryStream)
                .GetMethod(nameof(MemoryStream.ToArray), Type.EmptyTypes);

            var write = typeof(Stream)
                .GetMethod(nameof(Stream.Write), new[] { typeof(byte[]), typeof(int), typeof(int) });

            if (schema is BytesSchema)
            {
                inner = new WireFormatBytesSerializerRewriter(stream)
                    .VisitAndConvert(inner, GetType().Name);
            }

            var writer = Expression.Block(
                new[] { stream },
                Expression.Assign(stream, Expression.New(streamConstructor)),
                Expression.TryFinally(
                    Expression.Block(
                        Expression.Call(
                            stream,
                            write,
                            Expression.Constant(header),
                            Expression.Constant(0),
                            Expression.Constant(header.Length)),
                        Expression.Invoke(
                            inner,
                            value,
                            Expression.New(writerConstructor, stream))),
                    Expression.Call(stream, dispose)),
                Expression.Call(stream, toArray));

            return Expression.Lambda<Func<T, byte[]>>(writer, value).Compile();
        }
    }
}
