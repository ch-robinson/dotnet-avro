namespace Chr.Avro.Confluent
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using global::Confluent.Kafka.SyncOverAsync;
    using global::Confluent.SchemaRegistry;

    /// <summary>
    /// A collection of convenience methods for <see cref="ConsumerBuilder{TKey, TValue}" /> that
    /// configure Avro deserializers.
    /// </summary>
    public static class ConsumerBuilderExtensions
    {
        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use for Registry operations. The client should only be
        /// disposed after the consumer; the deserializer will use it to request schemas as
        /// messages are being consumed.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static ConsumerBuilder<TKey, TValue> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient)
        => consumerBuilder.SetKeyDeserializer(
            new AsyncSchemaRegistryDeserializer<TKey>(registryClient).AsSyncOverAsync());

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static ConsumerBuilder<TKey, TValue> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration)
        => consumerBuilder.SetKeyDeserializer(
            new AsyncSchemaRegistryDeserializer<TKey>(registryConfiguration).AsSyncOverAsync());

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            int id)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient);

            return await consumerBuilder
                .SetAvroKeyDeserializer(deserializerBuilder, id)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration);

            return await consumerBuilder
                .SetAvroKeyDeserializer(deserializerBuilder, id)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryDeserializerBuilder deserializerBuilder,
            int id)
        => consumerBuilder.SetKeyDeserializer(
            await deserializerBuilder.Build<TKey>(id).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient);

            return await consumerBuilder
                .SetAvroKeyDeserializer(deserializerBuilder, subject)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration);

            return await consumerBuilder
                .SetAvroKeyDeserializer(deserializerBuilder, subject)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject)
        => consumerBuilder.SetKeyDeserializer(
            await deserializerBuilder.Build<TKey>(subject).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient);

            return await consumerBuilder
                .SetAvroKeyDeserializer(deserializerBuilder, subject, version)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration);

            return await consumerBuilder
                .SetAvroKeyDeserializer(deserializerBuilder, subject, version)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
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
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroKeyDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject,
            int version)
        => consumerBuilder.SetKeyDeserializer(
            await deserializerBuilder.Build<TKey>(subject, version).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use for Registry operations. The client should only be
        /// disposed after the consumer; the deserializer will use it to request schemas as
        /// messages are being consumed.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static ConsumerBuilder<TKey, TValue> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => consumerBuilder.SetValueDeserializer(
            new AsyncSchemaRegistryDeserializer<TValue>(registryClient, tombstoneBehavior: tombstoneBehavior).AsSyncOverAsync());

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static ConsumerBuilder<TKey, TValue> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => consumerBuilder.SetValueDeserializer(
            new AsyncSchemaRegistryDeserializer<TValue>(registryConfiguration, tombstoneBehavior: tombstoneBehavior).AsSyncOverAsync());

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient);

            return await consumerBuilder
                .SetAvroValueDeserializer(deserializerBuilder, id, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration);

            return await consumerBuilder
                .SetAvroValueDeserializer(deserializerBuilder, id, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryDeserializerBuilder deserializerBuilder,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => consumerBuilder.SetValueDeserializer(
            await deserializerBuilder.Build<TValue>(id, tombstoneBehavior).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient);

            return await consumerBuilder
                .SetAvroValueDeserializer(deserializerBuilder, subject, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration);

            return await consumerBuilder
                .SetAvroValueDeserializer(deserializerBuilder, subject, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => consumerBuilder.SetValueDeserializer(
            await deserializerBuilder.Build<TValue>(subject, tombstoneBehavior).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryClient);

            return await consumerBuilder
                .SetAvroValueDeserializer(deserializerBuilder, subject, version, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// A Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var deserializerBuilder = new SchemaRegistryDeserializerBuilder(registryConfiguration);

            return await consumerBuilder
                .SetAvroValueDeserializer(deserializerBuilder, subject, version, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro deserializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be deserialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be deserialized.
        /// </typeparam>
        /// <param name="consumerBuilder">
        /// A <see cref="ConsumerBuilder{TKey, TValue}" /> instance to be configured.
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
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="consumerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ConsumerBuilder<TKey, TValue>> SetAvroValueDeserializer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> consumerBuilder,
            ISchemaRegistryDeserializerBuilder deserializerBuilder,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => consumerBuilder.SetValueDeserializer(
            await deserializerBuilder.Build<TValue>(subject, version, tombstoneBehavior).ConfigureAwait(false));
    }
}
