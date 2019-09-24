using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// A collection of <see cref="ConsumerBuilder{TKey, TValue}" /> convenience methods that
    /// configure Avro deserializers.
    /// </summary>
    public static class ConsumerBuilderExtensions
    {
        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. The client should only be disposed
        /// after the consumer; the deserializer will use it to request schemas as messages are
        /// being consumed.
        /// </param>
        public static ConsumerBuilder<TKey, TValue> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient
        ) => consumerBuilder.SetKeyDeserializer(
            new AsyncSchemaRegistryDeserializer<TKey>(registryClient).AsSyncOverAsync()
        );

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        public static ConsumerBuilder<TKey, TValue> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration
        ) => consumerBuilder.SetKeyDeserializer(
            new AsyncSchemaRegistryDeserializer<TKey>(registryConfiguration).AsSyncOverAsync()
        );

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize keys.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient))
            {
                return await consumerBuilder.SetAvroKeyDeserializer(deserializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize keys.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration))
            {
                return await consumerBuilder.SetAvroKeyDeserializer(deserializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize keys.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            SchemaRegistryDeserializerBuilder deserializerBuilder,
            int id
        ) => consumerBuilder.SetKeyDeserializer(await deserializerBuilder.Build<TKey>(id));

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys. The latest version
        /// of the subject will be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient))
            {
                return await consumerBuilder.SetAvroKeyDeserializer(deserializerBuilder, subject);
            }
        }

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys. The latest version
        /// of the subject will be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration))
            {
                return await consumerBuilder.SetAvroKeyDeserializer(deserializerBuilder, subject);
            }
        }

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            SchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject
        ) => consumerBuilder.SetKeyDeserializer(await deserializerBuilder.Build<TKey>(subject));


        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient))
            {
                return await consumerBuilder.SetAvroKeyDeserializer(deserializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration))
            {
                return await consumerBuilder.SetAvroKeyDeserializer(deserializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message key deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            SchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject,
            int version
        ) => consumerBuilder.SetKeyDeserializer(await deserializerBuilder.Build<TKey>(subject, version));

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. The client should only be disposed
        /// after the consumer; the deserializer will use it to request schemas as messages are
        /// being consumed.
        /// </param>
        public static ConsumerBuilder<TKey, TValue> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient
        ) => consumerBuilder.SetValueDeserializer(
            new AsyncSchemaRegistryDeserializer<TValue>(registryClient).AsSyncOverAsync()
        );

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        public static ConsumerBuilder<TKey, TValue> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration
        ) => consumerBuilder.SetValueDeserializer(
            new AsyncSchemaRegistryDeserializer<TValue>(registryConfiguration).AsSyncOverAsync()
        );

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize values.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient))
            {
                return await consumerBuilder.SetAvroValueDeserializer(deserializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize values.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration))
            {
                return await consumerBuilder.SetAvroValueDeserializer(deserializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize values.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            SchemaRegistryDeserializerBuilder deserializerBuilder,
            int id
        ) => consumerBuilder.SetValueDeserializer(await deserializerBuilder.Build<TValue>(id));

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient))
            {
                return await consumerBuilder.SetAvroValueDeserializer(deserializerBuilder, subject);
            }
        }

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration))
            {
                return await consumerBuilder.SetAvroValueDeserializer(deserializerBuilder, subject);
            }
        }

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            SchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject
        ) => consumerBuilder.SetValueDeserializer(await deserializerBuilder.Build<TValue>(subject));

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient))
            {
                return await consumerBuilder.SetAvroValueDeserializer(deserializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration))
            {
                return await consumerBuilder.SetAvroValueDeserializer(deserializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message value deserializer.
        /// </summary>
        /// <param name="consumerBuilder">
        /// The <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            SchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject,
            int version
        ) => consumerBuilder.SetValueDeserializer(await deserializerBuilder.Build<TValue>(subject, version));
    }
}
