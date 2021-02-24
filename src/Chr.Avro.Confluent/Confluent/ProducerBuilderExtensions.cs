namespace Chr.Avro.Confluent
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;

    /// <summary>
    /// A collection of convenience methods for <see cref="ProducerBuilder{TKey, TValue}" />
    /// and <see cref="DependentProducerBuilder{TKey, TValue}" /> that configure Avro serializers.
    /// </summary>
    public static class ProducerBuilderExtensions
    {
        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        ///
        ///
        ///
        ///
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use for Registry operations. The client should only be
        /// disposed after the producer; the serializer will use it to request schemas as
        /// messages are being produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static DependentProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null)
        => producerBuilder.SetKeySerializer(
            new AsyncSchemaRegistrySerializer<TKey>(
                registryClient,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        ///
        ///
        ///
        ///
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use for Registry operations. The client should only be
        /// disposed after the producer; the serializer will use it to request schemas as
        /// messages are being produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static ProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null)
        => producerBuilder.SetKeySerializer(
            new AsyncSchemaRegistrySerializer<TKey>(
                registryClient,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static DependentProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null)
        => producerBuilder.SetKeySerializer(
            new AsyncSchemaRegistrySerializer<TKey>(
                registryConfiguration,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static ProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null)
        => producerBuilder.SetKeySerializer(
            new AsyncSchemaRegistrySerializer<TKey>(
                registryConfiguration,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, id)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, id)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, id)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, id)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            int id)
        => producerBuilder.SetKeySerializer(
            await serializerBuilder.Build<TKey>(id).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            int id)
        => producerBuilder.SetKeySerializer(
            await serializerBuilder.Build<TKey>(id).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never)
        => producerBuilder.SetKeySerializer(
            await serializerBuilder.Build<TKey>(subject, registerAutomatically).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never)
        => producerBuilder.SetKeySerializer(
            await serializerBuilder.Build<TKey>(subject, registerAutomatically).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, version)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, version)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, version)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroKeySerializer(serializerBuilder, subject, version)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version)
        => producerBuilder.SetKeySerializer(
            await serializerBuilder.Build<TKey>(subject, version).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for keys.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TKey" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version)
        => producerBuilder.SetKeySerializer(
            await serializerBuilder.Build<TKey>(subject, version).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        ///
        ///
        ///
        ///
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use for Registry operations. The client should only be
        /// disposed after the producer; the serializer will use it to request schemas as
        /// messages are being produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static DependentProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            new AsyncSchemaRegistrySerializer<TValue>(
                registryClient,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder,
                tombstoneBehavior: tombstoneBehavior));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        ///
        ///
        ///
        ///
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use to resolve the schema. (The client will not be
        /// disposed.) use for Registry operations. The client should only be
        /// disposed after the producer; the serializer will use it to request schemas as
        /// messages are being produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static ProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            new AsyncSchemaRegistrySerializer<TValue>(
                registryClient,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder,
                tombstoneBehavior: tombstoneBehavior));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static DependentProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            new AsyncSchemaRegistrySerializer<TValue>(
                registryConfiguration,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder,
                tombstoneBehavior: tombstoneBehavior));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether the serializer should automatically register schemas that match
        /// <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines a subject name given a topic name and a component type (key
        /// or value). If none is provided, the default <c>{topic name}-{component}</c> naming
        /// convention will be used.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static ProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string> subjectNameBuilder = null,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            new AsyncSchemaRegistrySerializer<TValue>(
                registryConfiguration,
                registerAutomatically: registerAutomatically,
                subjectNameBuilder: subjectNameBuilder,
                tombstoneBehavior: tombstoneBehavior));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, id, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, id, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, id, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, id, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            await serializerBuilder.Build<TValue>(id, tombstoneBehavior).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            await serializerBuilder.Build<TValue>(id, tombstoneBehavior).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            await serializerBuilder.Build<TValue>(subject, registerAutomatically, tombstoneBehavior).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            await serializerBuilder.Build<TValue>(subject, registerAutomatically, tombstoneBehavior).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, version, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A Schema Registry client to use to resolve the schema. (The client will not be
        /// disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, version, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, version, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            using var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration);

            return await producerBuilder
                .SetAvroValueSerializer(serializerBuilder, subject, version, tombstoneBehavior)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            await serializerBuilder.Build<TValue>(subject, version, tombstoneBehavior).ConfigureAwait(false));

        /// <summary>
        /// Sets an Avro serializer for values.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of key to be serialized.
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of value to be serialized.
        /// </typeparam>
        /// <param name="producerBuilder">
        /// A <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        /// <returns>
        /// <paramref name="producerBuilder" /> with an Avro deserializer configured for
        /// <typeparamref name="TValue" />.
        /// </returns>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        => producerBuilder.SetValueSerializer(
            await serializerBuilder.Build<TValue>(subject, version, tombstoneBehavior).ConfigureAwait(false));
    }
}
