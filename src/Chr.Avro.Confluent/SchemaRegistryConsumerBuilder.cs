using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// A builder class for <see cref="Consumer{TKey, TValue}" /> instances that automatically
    /// configures Avro deserialization.
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
        /// Builds a new <see cref="Consumer{TKey, TValue}" /> instance.
        /// </summary>
        public override IConsumer<TKey, TValue> Build()
        {
            if (KeyDeserializer == null && AsyncKeyDeserializer == null && !IgnoredTypes.Contains(typeof(TKey)))
            {
                AsyncKeyDeserializer = new AsyncSchemaRegistryDeserializer<TKey>(_registryConfiguration);
            }

            if (ValueDeserializer == null && AsyncValueDeserializer == null && !IgnoredTypes.Contains(typeof(TValue)))
            {
                AsyncValueDeserializer = new AsyncSchemaRegistryDeserializer<TValue>(_registryConfiguration);
            }

            return base.Build();
        }
    }
}
