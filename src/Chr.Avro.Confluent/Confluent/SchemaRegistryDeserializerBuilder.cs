namespace Chr.Avro.Confluent
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Chr.Avro.Abstract;
    using Chr.Avro.Representation;
    using Chr.Avro.Serialization;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;

    /// <summary>
    /// Builds <see cref="T:IDeserializer{T}" />s based on specific schemas from a Schema Registry
    /// instance.
    /// </summary>
    public class SchemaRegistryDeserializerBuilder : ISchemaRegistryDeserializerBuilder, IDisposable
    {
        private readonly bool disposeRegistryClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaRegistryDeserializerBuilder" />
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
        public SchemaRegistryDeserializerBuilder(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null)
            : this(
                new CachedSchemaRegistryClient(registryConfiguration),
                deserializerBuilder,
                schemaReader)
        {
            disposeRegistryClient = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaRegistryDeserializerBuilder" />
        /// class with a Schema Registry instance.
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
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="registryClient" /> is <c>null</c>.
        /// </exception>
        public SchemaRegistryDeserializerBuilder(
            ISchemaRegistryClient registryClient,
            IBinaryDeserializerBuilder deserializerBuilder = null,
            IJsonSchemaReader schemaReader = null)
        {
            disposeRegistryClient = false;

            DeserializerBuilder = deserializerBuilder ?? new BinaryDeserializerBuilder();
            RegistryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
            SchemaReader = schemaReader ?? new JsonSchemaReader();
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

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <typeparamref name="T" /> is incompatible with the retrieved schema.
        /// </exception>
        /// <inheritdoc />
        public virtual async Task<IDeserializer<T>> Build<T>(
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
        public virtual async Task<IDeserializer<T>> Build<T>(
            string subject,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            var schema = await RegistryClient.GetLatestSchemaAsync(subject).ConfigureAwait(false);

            if (schema.SchemaType != SchemaType.Avro)
            {
                throw new UnsupportedSchemaException(null, $"The latest schema with subject {subject} is not an Avro schema.");
            }

            return Build<T>(schema.Id, schema.SchemaString, tombstoneBehavior);
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <typeparamref name="T" /> is incompatible with the retrieved schema.
        /// </exception>
        /// <inheritdoc />
        public virtual async Task<IDeserializer<T>> Build<T>(
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
        /// Builds a deserializer for the Confluent wire format.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be deserialized.
        /// </typeparam>
        /// <param name="id">
        /// A schema ID that all payloads must be serialized with. If a received schema ID does not
        /// match this ID, <see cref="InvalidEncodingException" /> will be thrown.
        /// </param>
        /// <param name="json">
        /// The schema to build the Avro deserializer from.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the deserializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="IDeserializer{T}" /> based on <paramref name="json" />.
        /// </returns>
        protected virtual IDeserializer<T> Build<T>(
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

                var hasNull = schema is NullSchema
                    || (schema is UnionSchema union && union.Schemas.Any(s => s is NullSchema));

                if (tombstoneBehavior == TombstoneBehavior.Strict && hasNull)
                {
                    throw new UnsupportedSchemaException(schema, "Tombstone deserialization is not supported for schemas that can represent null values.");
                }
            }

            return new DelegateDeserializer<T>(DeserializerBuilder.BuildDelegate<T>(schema), id, tombstoneBehavior);
        }
    }
}
