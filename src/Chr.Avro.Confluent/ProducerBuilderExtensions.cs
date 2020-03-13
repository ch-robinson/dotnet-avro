using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// A collection of convenience methods for <see cref="ProducerBuilder{TKey, TValue}" />
    /// and <see cref="DependentProducerBuilder{TKey, TValue}" /> that configure Avro serializers.
    /// </summary>
    public static class ProducerBuilderExtensions
    {
        #region SetAvroKeySerializer

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryClient,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryClient,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryConfiguration,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TKey" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryConfiguration,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            int id
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(id).ConfigureAwait(false));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            int id
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(id).ConfigureAwait(false));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TKey" />.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, registerAutomatically).ConfigureAwait(false));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, registerAutomatically).ConfigureAwait(false));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, version).ConfigureAwait(false));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, version).ConfigureAwait(false));

        #endregion

        #region SetAvroValueSerializer

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(
            registryClient,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(
            registryClient,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(
            registryConfiguration,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register schemas that match <typeparamref name="TValue" />.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            Func<SerializationContext, string>? subjectNameBuilder = null
        ) => producerBuilder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(
            registryConfiguration,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            int id
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(id).ConfigureAwait(false));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            int id
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(id).ConfigureAwait(false));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="TValue" />.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, registerAutomatically).ConfigureAwait(false));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, registerAutomatically).ConfigureAwait(false));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// The client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, version).ConfigureAwait(false));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, version).ConfigureAwait(false));
    }

    #endregion
}
