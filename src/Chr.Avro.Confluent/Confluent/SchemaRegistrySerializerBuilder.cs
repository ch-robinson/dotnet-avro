namespace Chr.Avro.Confluent
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Chr.Avro.Representation;
    using Chr.Avro.Serialization;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;

    /// <summary>
    /// Builds <see cref="T:ISerializer{T}" />s based on specific schemas from a Schema Registry
    /// instance.
    /// </summary>
    public class SchemaRegistrySerializerBuilder : ISchemaRegistrySerializerBuilder, IDisposable
    {
        private readonly bool disposeRegistryClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaRegistrySerializerBuilder" />
        /// class with a Schema Registry configuration.
        /// </summary>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
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
        /// The deserializer builder to use to build serialization functions for C# types. If none
        /// is provided, the default serializer builder will be used.
        /// </param>
        public SchemaRegistrySerializerBuilder(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null)
            : this(
                new CachedSchemaRegistryClient(registryConfiguration),
                schemaBuilder,
                schemaReader,
                schemaWriter,
                serializerBuilder)
        {
            disposeRegistryClient = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaRegistrySerializerBuilder" />
        /// class with a Schema Registry client.
        /// </summary>
        /// <param name="registryClient">
        /// A Schema Registry client to use for Registry operations. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that should be used to create a schemas when registering
        /// automatically. If none is provided, the default <see cref="SchemaBuilder" /> will be
        /// used.
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
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="registryClient" /> is <c>null</c>.
        /// </exception>
        public SchemaRegistrySerializerBuilder(
            ISchemaRegistryClient registryClient,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            IBinarySerializerBuilder serializerBuilder = null)
        {
            disposeRegistryClient = false;

            RegistryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
            SchemaBuilder = schemaBuilder ?? new Abstract.SchemaBuilder();
            SchemaReader = schemaReader ?? new JsonSchemaReader();
            SchemaWriter = schemaWriter ?? new JsonSchemaWriter();
            SerializerBuilder = serializerBuilder ?? new BinarySerializerBuilder();
        }

        /// <summary>
        /// Gets the Schema Registry client that will be used for Registry operations.
        /// </summary>
        public ISchemaRegistryClient RegistryClient { get; }

        /// <summary>
        /// Gets the schema builder instance that will be used to create <see cref="Schema" />s
        /// when registering automatically.
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

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <typeparamref name="T" /> is incompatible with the retrieved schema.
        /// </exception>
        /// <inheritdoc />
        public async Task<ISerializer<T>> Build<T>(
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            var schema = await RegistryClient.GetSchemaAsync(id).ConfigureAwait(false);

            if (schema.SchemaType != SchemaType.Avro)
            {
                throw new UnsupportedSchemaException(null, $"The schema with ID {id} is not an Avro schema.");
            }

            return Build<T>(id, schema.SchemaString, tombstoneBehavior);
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <typeparamref name="T" /> is incompatible with the retrieved schema.
        /// </exception>
        /// <inheritdoc />
        public async Task<ISerializer<T>> Build<T>(
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            switch (registerAutomatically)
            {
                case AutomaticRegistrationBehavior.Always:
                    var json = SchemaWriter.Write(SchemaBuilder.BuildSchema<T>());
                    var id = await RegistryClient.RegisterSchemaAsync(subject, new Schema(json, SchemaType.Avro)).ConfigureAwait(false);

                    return Build<T>(id, json, tombstoneBehavior);

                case AutomaticRegistrationBehavior.Never:
                    var existing = await RegistryClient.GetLatestSchemaAsync(subject).ConfigureAwait(false);

                    return Build<T>(existing.Id, existing.SchemaString, tombstoneBehavior);

                default:
                    throw new ArgumentOutOfRangeException(nameof(registerAutomatically));
            }
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <typeparamref name="T" /> is incompatible with the retrieved schema.
        /// </exception>
        /// <inheritdoc />
        public virtual async Task<ISerializer<T>> Build<T>(
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            var schema = await RegistryClient.GetRegisteredSchemaAsync(subject, version).ConfigureAwait(false);

            if (schema.SchemaType != SchemaType.Avro)
            {
                throw new UnsupportedSchemaException(null, $"The schema with subject {subject} and version {version} is not an Avro schema.");
            }

            var id = await RegistryClient.GetSchemaIdAsync(subject, schema).ConfigureAwait(false);

            return Build<T>(id, schema.SchemaString, tombstoneBehavior);
        }

        /// <summary>
        /// Disposes the builder, freeing up any resources.
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
        /// Builds a serializer for the Confluent wire format.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be deserialized.
        /// </typeparam>
        /// <param name="id">
        /// A schema ID to include in each serialized payload.
        /// </param>
        /// <param name="json">
        /// The schema to build the Avro serializer from.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="ISerializer{T}" /> based on <paramref name="json" />.
        /// </returns>
        protected virtual ISerializer<T> Build<T>(
            int id,
            string json,
            TombstoneBehavior tombstoneBehavior)
        {
            var schema = SchemaReader.Read(json);

            if (tombstoneBehavior != TombstoneBehavior.None)
            {
                if (default(T) != null)
                {
                    throw new UnsupportedTypeException(typeof(T), $"{typeof(T)} cannot represent tombstone values.");
                }

                var hasNull = schema is Abstract.NullSchema
                    || (schema is Abstract.UnionSchema union && union.Schemas.Any(s => s is Abstract.NullSchema));

                if (tombstoneBehavior == TombstoneBehavior.Strict && hasNull)
                {
                    throw new UnsupportedSchemaException(schema, "Tombstone serialization is not supported for schemas that can represent null values.");
                }
            }

            return new DelegateSerializer<T>(SerializerBuilder.BuildDelegate<T>(schema), id, tombstoneBehavior);
        }
    }
}
