using Chr.Avro.Representation;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// Builds <see cref="T:ISerializer{T}" />s based on specific schemas from a Schema Registry
    /// instance.
    /// </summary>
    public interface ISchemaRegistrySerializerBuilder
    {
        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize data.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        Task<ISerializer<T>> Build<T>(
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        );

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="T" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        Task<ISerializer<T>> Build<T>(
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        );

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        Task<ISerializer<T>> Build<T>(
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        );
    }

    /// <summary>
    /// Builds <see cref="T:ISerializer{T}" />s based on specific schemas from a Schema Registry
    /// instance.
    /// </summary>
    public class SchemaRegistrySerializerBuilder : ISchemaRegistrySerializerBuilder, IDisposable
    {
        /// <summary>
        /// The client to use for Schema Registry operations.
        /// </summary>
        public ISchemaRegistryClient RegistryClient { get; }

        /// <summary>
        /// The schema builder used to create a schema for a C# type when registering automatically.
        /// </summary>
        public Abstract.ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// The JSON schema reader used to convert schemas received from the registry into abstract
        /// representations.
        /// </summary>
        public IJsonSchemaReader SchemaReader { get; }

        /// <summary>
        /// The JSON schema writer used to convert abstract schema representations when registering
        /// automatically.
        /// </summary>
        public IJsonSchemaWriter SchemaWriter { get; }

        /// <summary>
        /// The deserializer builder to use to build serialization functions for C# types.
        /// </summary>
        public Serialization.IBinarySerializerBuilder SerializerBuilder { get; }

        private readonly bool _disposeRegistryClient;

        /// <summary>
        /// Creates a serializer builder.
        /// </summary>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="schemaBuilder">
        /// The schema builder to use to create a schema for a C# type when registering automatically.
        /// If none is provided, the default schema builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// The JSON schema reader to use to convert schemas received from the registry into abstract
        /// representations. If none is provided, the default schema reader will be used.
        /// </param>
        /// <param name="schemaWriter">
        /// The JSON schema writer to use to convert abstract schema representations when registering
        /// automatically. If none is provided, the default schema writer will be used.
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
            Serialization.IBinarySerializerBuilder serializerBuilder = null
        ) : this(
            new CachedSchemaRegistryClient(registryConfiguration),
            schemaBuilder,
            schemaReader,
            schemaWriter,
            serializerBuilder
        ) {
            _disposeRegistryClient = true;
        }

        /// <summary>
        /// Creates a serializer builder.
        /// </summary>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <param name="schemaBuilder">
        /// The schema builder to use to create a schema for a C# type when registering automatically.
        /// If none is provided, the default schema builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// The JSON schema reader to use to convert schemas received from the registry into abstract
        /// representations. If none is provided, the default schema reader will be used.
        /// </param>
        /// <param name="schemaWriter">
        /// The JSON schema writer to use to convert abstract schema representations when registering
        /// automatically. If none is provided, the default schema writer will be used.
        /// </param>
        /// <param name="serializerBuilder">
        /// The deserializer builder to use to build serialization functions for C# types. If none
        /// is provided, the default serializer builder will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public SchemaRegistrySerializerBuilder(
            ISchemaRegistryClient registryClient,
            Abstract.ISchemaBuilder schemaBuilder = null,
            IJsonSchemaReader schemaReader = null,
            IJsonSchemaWriter schemaWriter = null,
            Serialization.IBinarySerializerBuilder serializerBuilder = null
        ) {

            _disposeRegistryClient = false;

            RegistryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
            SchemaBuilder = schemaBuilder ?? new Abstract.SchemaBuilder();
            SchemaReader = schemaReader ?? new JsonSchemaReader();
            SchemaWriter = schemaWriter ?? new JsonSchemaWriter();
            SerializerBuilder = serializerBuilder ?? new Serialization.BinarySerializerBuilder();
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize data.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        /// <exception cref="AggregateException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<ISerializer<T>> Build<T>(
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        ) {
            var schema = await RegistryClient.GetSchemaAsync(id).ConfigureAwait(false);

            if (schema.SchemaType != SchemaType.Avro)
            {
                throw new UnsupportedSchemaException(null, $"The schema with ID {id} is not an Avro schema.");
            }

            return Build<T>(id, schema.SchemaString, tombstoneBehavior);
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="T" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        /// <exception cref="AggregateException">
        /// Thrown when the type is incompatible with the retrieved schema or a matching schema
        /// cannot be generated.
        /// </exception>
        public async Task<ISerializer<T>> Build<T>(
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        ) {
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

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        /// <exception cref="AggregateException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public virtual async Task<ISerializer<T>> Build<T>(
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None
        ) {
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
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the builder, freeing up any resources.
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

        /// <summary>
        /// Builds a serializer for the Confluent wire format.
        /// </summary>
        /// <param name="id">
        /// A schema ID to include in each serialized payload.
        /// </param>
        /// <param name="json">
        /// The schema to build the Avro serializer from.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        protected virtual ISerializer<T> Build<T>(
            int id,
            string json,
            TombstoneBehavior tombstoneBehavior
        ) {
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

            var bytes = BitConverter.GetBytes(id);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            var serialize = SerializerBuilder.BuildDelegate<T>(schema);

            return new DelegateSerializer<T>((data, context) =>
            {
                if (data == null && tombstoneBehavior != TombstoneBehavior.None)
                {
                    if (context.Component == MessageComponentType.Value || tombstoneBehavior != TombstoneBehavior.Strict)
                    {
                        return null;
                    }
                }

                var stream = new MemoryStream();

                using (stream)
                {
                    stream.WriteByte(0x00);
                    stream.Write(bytes, 0, bytes.Length);

                    serialize(data, stream);
                }

                return stream.ToArray();
            });
        }
    }
}
