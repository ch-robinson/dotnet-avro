using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// A builder class for <see cref="Producer{TKey, TValue}" /> instances that automatically
    /// configures Avro serialization.
    /// </summary>
    public class SchemaRegistryProducerBuilder<TKey, TValue> : ProducerBuilder<TKey, TValue>
    {
        /// <summary>
        /// A collection of types that Avro serializers should not be created for.
        /// </summary>
        protected static readonly ICollection<Type> IgnoredTypes = new HashSet<Type>
        {
            typeof(Ignore),
            typeof(Null)
        };

        private readonly IEnumerable<KeyValuePair<string, string>> _registryConfiguration;

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="producerConfiguration">
        /// A collection of configuration parameters for librdkafka producers. See the librdkafka
        /// docs for native client options and <see cref="ConfigPropertyNames" /> for Confluent-specific
        /// options. Using the <see cref="ProducerConfig" /> class is highly recommended.
        /// </param>
        /// <param name="registryConfiguration">
        /// Configuration parameters to use when creating Schema Registry deserializers. Using the
        /// <see cref="SchemaRegistryConfig" /> class is highly recommended.
        /// </param>
        public SchemaRegistryProducerBuilder(
            IEnumerable<KeyValuePair<string, string>> producerConfiguration,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration
        ) : base(producerConfiguration)
        {
            _registryConfiguration = registryConfiguration;
        }

        /// <summary>
        /// Builds a new <see cref="Producer{TKey, TValue}" /> instance.
        /// </summary>
        public override Producer<TKey, TValue> Build()
        {
            if (KeySerializer == null && AsyncKeySerializer == null && !IgnoredTypes.Contains(typeof(TKey)))
            {
                AsyncKeySerializer = new AsyncSchemaRegistrySerializer<TKey>(_registryConfiguration);
            }

            if (ValueSerializer == null && AsyncValueSerializer == null && !IgnoredTypes.Contains(typeof(TValue)))
            {
                AsyncValueSerializer = new AsyncSchemaRegistrySerializer<TValue>(_registryConfiguration);
            }

            return base.Build();
        }
    }
}
