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

        /// <summary>
        /// Creates a consumer builder.
        /// </summary>
        /// <param name="configuration">
        /// A collection of configuration parameters for librdkafka consumers. See the librdkafka
        /// docs for native client options, <see cref="ConfigPropertyNames" /> for Confluent-specific
        /// options, and <see cref="SchemaRegistryConfig.PropertyNames" /> for Schema Registry options.
        /// </param>
        public SchemaRegistryConsumerBuilder(IEnumerable<KeyValuePair<string, string>> configuration) : base(configuration) { }

        /// <summary>
        /// Builds a new <see cref="Consumer{TKey, TValue}" /> instance.
        /// </summary>
        public override Consumer<TKey, TValue> Build()
        {
            if (KeyDeserializer == null && AsyncKeyDeserializer == null && !IgnoredTypes.Contains(typeof(TKey)))
            {
                AsyncKeyDeserializer = new AsyncSchemaRegistryDeserializer<TKey>(Config);
            }

            if (ValueDeserializer == null && AsyncValueDeserializer == null && !IgnoredTypes.Contains(typeof(TValue)))
            {
                AsyncValueDeserializer = new AsyncSchemaRegistryDeserializer<TValue>(Config);
            }

            return base.Build();
        }
    }
}
