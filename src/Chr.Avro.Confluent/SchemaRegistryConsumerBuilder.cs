using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// A builder class for <see cref="IConsumer{TKey, TValue}" />s that automatically configures
    /// Avro deserialization.
    /// </summary>
    public class SchemaRegistryConsumerBuilder<TKey, TValue> : ConsumerBuilder<TKey, TValue>
    {
        /// <summary>
        /// A collection of types that Avro deserializers should not be created for.
        /// </summary>
        protected static readonly ICollection<Type> IgnoredTypes = new HashSet<Type>
        {
            typeof(Ignore),
            typeof(Null)
        };

        private readonly IEnumerable<KeyValuePair<string, string>> _registryConfiguration;

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="consumerConfiguration">
        /// A collection of configuration parameters for librdkafka consumers. See the librdkafka
        /// docs for native client options and <see cref="ConfigPropertyNames" /> for Confluent-specific
        /// options. Using the <see cref="ConsumerConfig" /> class is highly recommended.
        /// </param>
        /// <param name="registryConfiguration">
        /// Configuration parameters to use when creating Schema Registry deserializers. Using the
        /// <see cref="SchemaRegistryConfig" /> class is highly recommended.
        /// </param>
        public SchemaRegistryConsumerBuilder(
            IEnumerable<KeyValuePair<string, string>> consumerConfiguration,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration
        ) : base(consumerConfiguration)
        {
            _registryConfiguration = registryConfiguration;
        }

        /// <summary>
        /// Builds a new <see cref="IConsumer{TKey, TValue}" />. If key and value deserializers
        /// have not been manually set, they will be configured automatically here with new
        /// <see cref="T:Chr.Avro.Confluent.AsyncSchemaRegistryDeserializer`1" /> instances.
        /// </summary>
        public override IConsumer<TKey, TValue> Build()
        {
            if (KeyDeserializer == null && !IgnoredTypes.Contains(typeof(TKey)))
            {
                KeyDeserializer = new AsyncSchemaRegistryDeserializer<TKey>(_registryConfiguration)
                    .AsSyncOverAsync();
            }

            if (ValueDeserializer == null && !IgnoredTypes.Contains(typeof(TValue)))
            {
                ValueDeserializer = new AsyncSchemaRegistryDeserializer<TValue>(_registryConfiguration)
                    .AsSyncOverAsync();
            }

            return base.Build();
        }
    }
}
