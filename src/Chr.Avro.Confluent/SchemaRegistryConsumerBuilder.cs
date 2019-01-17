using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// Builds <see cref="IConsumer{TKey, TValue}" />s that use schemas from a Schema Registry to
    /// deserialize records.
    /// </summary>
    public interface ISchemaRegistryConsumerBuilder
    {
        /// <summary>
        /// Builds a Kafka consumer that resolves schemas on the fly.
        /// </summary>
        IConsumer<TKey, TValue> BuildConsumer<TKey, TValue>();

        /// <summary>
        /// Builds a Kafka consumer bound to specific key and value schemas.
        /// </summary>
        /// <param name="keySchemaId">
        /// The ID of the schema to use to deserialize the record key.
        /// </param>
        /// <param name="valueSchemaId">
        /// The ID of the schema to use to deserialize the record value.
        /// </param>
        Task<IConsumer<TKey, TValue>> BuildConsumer<TKey, TValue>(int keySchemaId, int valueSchemaId);

        /// <summary>
        /// Builds a Kafka consumer bound to specific key and value schemas.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to deserialize the record key. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to deserialize the record value. The latest version of
        /// the subject will be resolved.
        /// </param>
        Task<IConsumer<TKey, TValue>> BuildConsumer<TKey, TValue>(string keySchemaSubject, string valueSchemaSubject);

        /// <summary>
        /// Builds a Kafka consumer bound to specific key and value schemas.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to deserialize the record key.
        /// </param>
        /// <param name="keySchemaVersion">
        /// The version of the record key schema to be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to deserialize the record value.
        /// </param>
        /// <param name="valueSchemaVersion">
        /// The version of the record value schema to be resolved.
        /// </param>
        Task<IConsumer<TKey, TValue>> BuildConsumer<TKey, TValue>(string keySchemaSubject, int keySchemaVersion, string valueSchemaSubject, int valueSchemaVersion);
    }

    /// <summary>
    /// Builds <see cref="IConsumer{TKey, TValue}" />s that use schemas from a Schema Registry to
    /// deserialize records.
    /// </summary>
    public class SchemaRegistryConsumerBuilder : ISchemaRegistryConsumerBuilder, IDisposable
    {
        private readonly bool _disposeDeserializerBuilder;

        /// <summary>
        /// Configuration options to use when instantiating consumers.
        /// </summary>
        protected readonly ConsumerConfig ConsumerConfiguration;

        /// <summary>
        /// The builder to get key/value deserializers from.
        /// </summary>
        protected readonly ISchemaRegistryDeserializerBuilder DeserializerBuilder;

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="consumerConfiguration">
        /// Configuration options to use when instantiating consumers.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will
        /// not be disposed.)
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the consumer configuration or registry client is null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(IEnumerable<KeyValuePair<string, string>> consumerConfiguration, ISchemaRegistryClient registryClient)
        {
            if (consumerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(consumerConfiguration));
            }

            if (registryClient == null)
            {
                throw new ArgumentNullException(nameof(registryClient));
            }

            ConsumerConfiguration = new ConsumerConfig(consumerConfiguration);
            DeserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient);

            _disposeDeserializerBuilder = true;
        }

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="consumerConfiguration">
        /// Configuration options to use when instantiating consumers.
        /// </param>
        /// <param name="deserializerBuilder">
        /// The builder to get key/value deserializers from.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the consumer configuration or deserializer builder is null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(IEnumerable<KeyValuePair<string, string>> consumerConfiguration, ISchemaRegistryDeserializerBuilder deserializerBuilder)
        {
            if (consumerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(consumerConfiguration));
            }

            ConsumerConfiguration = new ConsumerConfig(consumerConfiguration);
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder));

            _disposeDeserializerBuilder = false;
        }

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="consumerConfiguration">
        /// Configuration options to use when instantiating consumers.
        /// </param>
        /// <param name="registryConfiguration">
        /// Configuration to use when connecting to the Schema Registry.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the consumer configuration or registry configuration is null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(IEnumerable<KeyValuePair<string, string>> consumerConfiguration, IEnumerable<KeyValuePair<string, string>> registryConfiguration)
        {
            if (consumerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(consumerConfiguration));
            }

            if (registryConfiguration == null)
            {
                throw new ArgumentNullException(nameof(registryConfiguration));
            }

            ConsumerConfiguration = new ConsumerConfig(consumerConfiguration);
            DeserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration);

            _disposeDeserializerBuilder = true;
        }

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="consumerConfiguration">
        /// Configuration options to use when instantiating consumers.
        /// </param>
        /// <param name="registryUrl">
        /// The URL of the Schema Registry to retrieve schemas from.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the consumer configuration or registry URL is null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(IEnumerable<KeyValuePair<string, string>> consumerConfiguration, string registryUrl) :
            this(
                consumerConfiguration,
                new SchemaRegistryConfig
                {
                    SchemaRegistryUrl = registryUrl
                        ?? throw new ArgumentNullException(nameof(registryUrl))
                }
            ) { }

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that consumers will be configured
        /// to connect to.
        /// </param>
        /// <param name="groupId">
        /// The consumer group that consumers will be be configured with.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will
        /// not be disposed.)
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the bootstrap server list, the group ID, or the registry
        /// client is null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(string bootstrapServers, string groupId, ISchemaRegistryClient registryClient) :
            this(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers
                        ?? throw new ArgumentNullException(nameof(bootstrapServers)),
                    GroupId = groupId
                        ?? throw new ArgumentNullException(nameof(groupId))
                },
                registryClient
            ) { }

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that consumers will be configured
        /// to connect to.
        /// </param>
        /// <param name="groupId">
        /// The consumer group that consumers will be be configured with.
        /// </param>
        /// <param name="deserializerBuilder">
        /// The builder to get key/value deserializers from.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the bootstrap server list, the group ID, or the deserializer builder is
        /// null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(string bootstrapServers, string groupId, ISchemaRegistryDeserializerBuilder deserializerBuilder) :
            this(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers
                        ?? throw new ArgumentNullException(nameof(bootstrapServers)),
                    GroupId = groupId
                        ?? throw new ArgumentNullException(nameof(groupId))
                },
                deserializerBuilder
            ) { }

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that consumers will be configured
        /// to connect to.
        /// </param>
        /// <param name="groupId">
        /// The consumer group that consumers will be be configured with.
        /// </param>
        /// <param name="registryConfiguration">
        /// Configuration to use when connecting to the Schema Registry.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the bootstrap server list, the group ID, or the registry configuration is
        /// null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(string bootstrapServers, string groupId, IEnumerable<KeyValuePair<string, string>> registryConfiguration) :
            this(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers
                        ?? throw new ArgumentNullException(nameof(bootstrapServers)),
                    GroupId = groupId
                        ?? throw new ArgumentNullException(nameof(groupId))
                },
                registryConfiguration
            ) { }

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that consumers will be configured
        /// to connect to.
        /// </param>
        /// <param name="groupId">
        /// The consumer group that consumers will be be configured with.
        /// </param>
        /// <param name="registryUrl">
        /// The URL of the Schema Registry to retrieve schemas from.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the bootstrap server list, the group ID, or the registry URL is null.
        /// </exception>
        public SchemaRegistryConsumerBuilder(string bootstrapServers, string groupId, string registryUrl) :
            this(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers
                        ?? throw new ArgumentNullException(nameof(bootstrapServers)),
                    GroupId = groupId
                        ?? throw new ArgumentNullException(nameof(groupId))
                },
                registryUrl
            ) { }

        /// <summary>
        /// Disposes the builder, freeing up any resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposeDeserializerBuilder)
            {
                ((IDisposable)DeserializerBuilder)?.Dispose();
            }
        }

        /// <summary>
        /// Builds a Kafka consumer that resolves schemas on the fly.
        /// </summary>
        public virtual IConsumer<TKey, TValue> BuildConsumer<TKey, TValue>()
        {
            return BuildConsumer(
                DeserializerBuilder.BuildDeserializer<TKey>(),
                DeserializerBuilder.BuildDeserializer<TValue>()
            );
        }

        /// <summary>
        /// Builds a Kafka consumer bound to specific key and value schemas.
        /// </summary>
        /// <param name="keySchemaId">
        /// The ID of the schema to use to deserialize the record key.
        /// </param>
        /// <param name="valueSchemaId">
        /// The ID of the schema to use to deserialize the record value.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the key or value type is incompatible with the respective
        /// schema.
        /// </exception>
        public virtual async Task<IConsumer<TKey, TValue>> BuildConsumer<TKey, TValue>(int keySchemaId, int valueSchemaId)
        {
            return BuildConsumer(
                await DeserializerBuilder.BuildDeserializer<TKey>(keySchemaId),
                await DeserializerBuilder.BuildDeserializer<TValue>(valueSchemaId)
            );
        }

        /// <summary>
        /// Builds a Kafka consumer bound to specific key and value schemas.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to deserialize the record key. The
        /// latest version of the subject will be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to deserialize the record value.
        /// The latest version of the subject will be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the key or value type is incompatible with the respective
        /// schema.
        /// </exception>
        public virtual async Task<IConsumer<TKey, TValue>> BuildConsumer<TKey, TValue>(string keySchemaSubject, string valueSchemaSubject)
        {
            return BuildConsumer(
                await DeserializerBuilder.BuildDeserializer<TKey>(keySchemaSubject),
                await DeserializerBuilder.BuildDeserializer<TValue>(valueSchemaSubject)
            );
        }

        /// <summary>
        /// Builds a Kafka consumer bound to specific key and value schemas.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to deserialize the record key.
        /// </param>
        /// <param name="keySchemaVersion">
        /// The version of the record key schema to be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to deserialize the record value.
        /// </param>
        /// <param name="valueSchemaVersion">
        /// The version of the record value schema to be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the key or value type is incompatible with the respective
        /// schema.
        /// </exception>
        public virtual async Task<IConsumer<TKey, TValue>> BuildConsumer<TKey, TValue>(string keySchemaSubject, int keySchemaVersion, string valueSchemaSubject, int valueSchemaVersion)
        {
            return BuildConsumer(
                await DeserializerBuilder.BuildDeserializer<TKey>(keySchemaSubject, keySchemaVersion),
                await DeserializerBuilder.BuildDeserializer<TValue>(valueSchemaSubject, valueSchemaVersion)
            );
        }

        private IConsumer<TKey, TValue> BuildConsumer<TKey, TValue>(Deserializer<TKey> keyDeserializer, Deserializer<TValue> valueDeserializer)
        {
            return new Consumer<TKey, TValue>(ConsumerConfiguration, keyDeserializer, valueDeserializer);
        }
    }
}
