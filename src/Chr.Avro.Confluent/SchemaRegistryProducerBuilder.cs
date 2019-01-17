using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
     /// Builds <see cref="IProducer{TKey, TValue}" />s that use schemas from a Schema Registry to
     /// serialize records.
     /// </summary>
    public interface ISchemaRegistryProducerBuilder
    {
        /// <summary>
        /// Builds a Kafka producer for a schema.
        /// </summary>
        /// <param name="keySchemaId">
        /// The ID of the schema to use to serialize the record key.
        /// </param>
        /// <param name="valueSchemaId">
        /// The ID of the schema to use to serialize the record value.
        /// </param>
        Task<IProducer<TKey, TValue>> BuildProducer<TKey, TValue>(int keySchemaId, int valueSchemaId);

        /// <summary>
        /// Builds a Kafka producer for a schema.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to serialize the record key. The
        /// latest version of the subject will be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to serialize the record value. The
        /// latest version of the subject will be resolved.
        /// </param>
        Task<IProducer<TKey, TValue>> BuildProducer<TKey, TValue>(string keySchemaSubject, string valueSchemaSubject);

        /// <summary>
        /// Builds a Kafka producer for a schema.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to serialize the record key.
        /// </param>
        /// <param name="keySchemaVersion">
        /// The version of the record key schema to be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to serialize the record value.
        /// </param>
        /// <param name="valueSchemaVersion">
        /// The version of the record value schema to be resolved.
        /// </param>
        Task<IProducer<TKey, TValue>> BuildProducer<TKey, TValue>(string keySchemaSubject, int keySchemaVersion, string valueSchemaSubject, int valueSchemaVersion);
    }

    /// <summary>
    /// Builds <see cref="IProducer{TKey, TValue}" />s that use schemas from a Schema Registry to
    /// serialize records.
    /// </summary>
    public class SchemaRegistryProducerBuilder : ISchemaRegistryProducerBuilder, IDisposable
    {
        private readonly bool _disposeSerializerBuilder;

        /// <summary>
        /// Configuration options to use when instantiating producers.
        /// </summary>
        protected readonly ProducerConfig ProducerConfiguration;

        /// <summary>
        /// The builder to get key/value serializers from.
        /// </summary>
        protected readonly ISchemaRegistrySerializerBuilder SerializerBuilder;

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="producerConfiguration">
        /// Configuration options to use when instantiating producers.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the producer configuration or registry client is null.
        /// </exception>
        public SchemaRegistryProducerBuilder(IEnumerable<KeyValuePair<string, string>> producerConfiguration, ISchemaRegistryClient registryClient)
        {
            if (producerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(producerConfiguration));
            }

            if (registryClient == null)
            {
                throw new ArgumentNullException(nameof(registryClient));
            }

            ProducerConfiguration = new ProducerConfig(producerConfiguration);
            SerializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            _disposeSerializerBuilder = true;
        }

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="producerConfiguration">
        /// Configuration options to use when instantiating producers.
        /// </param>
        /// <param name="serializerBuilder">
        /// The builder to get key/value serializers from.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the producer configuration or serializer builder is null.
        /// </exception>
        public SchemaRegistryProducerBuilder(IEnumerable<KeyValuePair<string, string>> producerConfiguration, ISchemaRegistrySerializerBuilder serializerBuilder)
        {
            if (producerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(producerConfiguration));
            }

            ProducerConfiguration = new ProducerConfig(producerConfiguration);
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder));

            _disposeSerializerBuilder = false;
        }

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="producerConfiguration">
        /// Configuration options to use when instantiating producers.
        /// </param>
        /// <param name="registryConfiguration">
        /// Configuration to use when connecting to the Schema Registry.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the producer configuration or registry configuration is
        /// null.
        /// </exception>
        public SchemaRegistryProducerBuilder(IEnumerable<KeyValuePair<string, string>> producerConfiguration, IEnumerable<KeyValuePair<string, string>> registryConfiguration)
        {
            if (producerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(producerConfiguration));
            }

            if (registryConfiguration == null)
            {
                throw new ArgumentNullException(nameof(registryConfiguration));
            }

            ProducerConfiguration = new ProducerConfig(producerConfiguration);
            SerializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            _disposeSerializerBuilder = true;
        }

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="producerConfiguration">
        /// Configuration options to use when instantiating producers.
        /// </param>
        /// <param name="registryUrl">
        /// The URL of the Schema Registry to retrieve schemas from.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the producer configuration or registry URL is null.
        /// </exception>
        public SchemaRegistryProducerBuilder(IEnumerable<KeyValuePair<string, string>> producerConfiguration, string registryUrl) :
            this(
                producerConfiguration,
                new SchemaRegistryConfig
                {
                    SchemaRegistryUrl = registryUrl
                        ?? throw new ArgumentNullException(nameof(registryUrl))
                }
            ) { }

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that producers will be configured
        /// to connect to.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will
        /// not be disposed.)
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the bootstrap server list or the registry client is
        /// null.
        /// </exception>
        public SchemaRegistryProducerBuilder(string bootstrapServers, ISchemaRegistryClient registryClient) :
            this(
                new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                        ?? throw new ArgumentNullException(nameof(bootstrapServers))
                },
                registryClient
            ) { }

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that producers will be configured
        /// to connect to.
        /// </param>
        /// <param name="serializerBuilder">
        /// The builder to get key/value serializers from.
        /// </param>
        /// <exception cref="System.ArgumentNullException">
        /// Thrown when the bootstrap server list or the serializer builder is null.
        /// </exception>
        public SchemaRegistryProducerBuilder(string bootstrapServers, ISchemaRegistrySerializerBuilder serializerBuilder) :
            this(
                new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                        ?? throw new ArgumentNullException(nameof(bootstrapServers)),
                },
                serializerBuilder
            ) { }

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that producers will be configured
        /// to connect to.
        /// </param>
        /// <param name="registryConfiguration">
        /// Configuration to use when connecting to the Schema Registry.
        /// </param>
        /// <exception cref="System.ArgumentNullException">
        /// Thrown when the bootstrap server list or the registry configuration
        /// is null.
        /// </exception>
        public SchemaRegistryProducerBuilder(string bootstrapServers, IEnumerable<KeyValuePair<string, string>> registryConfiguration) :
            this(
                new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                },
                registryConfiguration
            ) { }

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="bootstrapServers">
        /// A comma-separated list of servers that producers will be configured
        /// to connect to.
        /// </param>
        /// <param name="registryUrl">
        /// The URL of the Schema Registry to retrieve schemas from.
        /// </param>
        /// <exception cref="System.ArgumentNullException">
        /// Thrown when the bootstrap server list or the registry URL is null.
        /// </exception>
        public SchemaRegistryProducerBuilder(string bootstrapServers, string registryUrl) :
            this(
                new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                },
                registryUrl
            ) { }

        /// <summary>
        /// Disposes the builder, freeing up any resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposeSerializerBuilder)
            {
                ((IDisposable)SerializerBuilder)?.Dispose();
            }
        }

        /// <summary>
        /// Builds a Kafka producer for a schema.
        /// </summary>
        /// <param name="keySchemaId">
        /// The ID of the schema to use to serialize the record key.
        /// </param>
        /// <param name="valueSchemaId">
        /// The ID of the schema to use to serialize the record value.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the key or value type is incompatible with the respective
        /// schema.
        /// </exception>
        public async Task<IProducer<TKey, TValue>> BuildProducer<TKey, TValue>(int keySchemaId, int valueSchemaId)
        {
            return BuildProducer(
                await SerializerBuilder.BuildSerializer<TKey>(keySchemaId),
                await SerializerBuilder.BuildSerializer<TValue>(valueSchemaId)
            );
        }

        /// <summary>
        /// Builds a Kafka producer for a schema.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to serialize the record key. The
        /// latest version of the subject will be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to serialize the record value. The
        /// latest version of the subject will be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the key or value type is incompatible with the respective
        /// schema.
        /// </exception>
        public async Task<IProducer<TKey, TValue>> BuildProducer<TKey, TValue>(string keySchemaSubject, string valueSchemaSubject)
        {
            return BuildProducer(
                await SerializerBuilder.BuildSerializer<TKey>(keySchemaSubject),
                await SerializerBuilder.BuildSerializer<TValue>(valueSchemaSubject)
            );
        }

        /// <summary>
        /// Builds a Kafka producer for a schema.
        /// </summary>
        /// <param name="keySchemaSubject">
        /// The subject of the schema to use to serialize the record key.
        /// </param>
        /// <param name="keySchemaVersion">
        /// The version of the record key schema to be resolved.
        /// </param>
        /// <param name="valueSchemaSubject">
        /// The subject of the schema to use to serialize the record value.
        /// </param>
        /// <param name="valueSchemaVersion">
        /// The version of the record value schema to be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the key or value type is incompatible with the respective
        /// schema.
        /// </exception>
        public async Task<IProducer<TKey, TValue>> BuildProducer<TKey, TValue>(string keySchemaSubject, int keySchemaVersion, string valueSchemaSubject, int valueSchemaVersion)
        {
            return BuildProducer(
                await SerializerBuilder.BuildSerializer<TKey>(keySchemaSubject, keySchemaVersion),
                await SerializerBuilder.BuildSerializer<TValue>(valueSchemaSubject, valueSchemaVersion)
            );
        }

        private IProducer<TKey, TValue> BuildProducer<TKey, TValue>(Serializer<TKey> keySerializer, Serializer<TValue> valueSerializer)
        {
            return new Producer<TKey, TValue>(ProducerConfiguration, keySerializer, valueSerializer);
        }
    }
}
