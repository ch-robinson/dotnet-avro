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

        /// <summary>
        /// Creates a producer builder.
        /// </summary>
        /// <param name="configuration">
        /// A collection of configuration parameters for librdkafka producers. See the librdkafka
        /// docs for native client options, <see cref="ConfigPropertyNames" /> for Confluent-specific
        /// options, and <see cref="SchemaRegistryConfig.PropertyNames" /> for Schema Registry options.
        /// </param>
        public SchemaRegistryProducerBuilder(IEnumerable<KeyValuePair<string, string>> configuration) : base(configuration) { }

        /// <summary>
        /// Builds a new <see cref="Producer{TKey, TValue}" /> instance.
        /// </summary>
        public override Producer<TKey, TValue> Build()
        {
            if (KeySerializer == null && AsyncKeySerializer == null && !IgnoredTypes.Contains(typeof(TKey)))
            {
                AsyncKeySerializer = new AsyncSchemaRegistrySerializer<TKey>(Config);
            }

            if (ValueSerializer == null && AsyncValueSerializer == null && !IgnoredTypes.Contains(typeof(TValue)))
            {
                AsyncValueSerializer = new AsyncSchemaRegistrySerializer<TValue>(Config);
            }

            return base.Build();
        }
    }
}
