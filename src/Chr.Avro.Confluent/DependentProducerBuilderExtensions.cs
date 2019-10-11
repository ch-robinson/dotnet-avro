using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// A collection of <see cref="DependentProducerBuilder{TKey, TValue}" /> convenience methods that
    /// configure Avro serializers.
    /// </summary>
    public static class DependentProducerBuilderExtensions
    {
        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            bool registerAutomatically = false,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) => DependentProducerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryClient,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            bool registerAutomatically = false,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) => DependentProducerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryConfiguration,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await DependentProducerBuilder.SetAvroKeySerializer(serializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await DependentProducerBuilder.SetAvroKeySerializer(serializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize keys.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            int id
        ) => DependentProducerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(id));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register a schema that matches <typeparamref name="TKey" />
        /// if one does not already exist.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            bool registerAutomatically = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await DependentProducerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
        /// Whether to automatically register a schema that matches <typeparamref name="TKey" />
        /// if one does not already exist.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            bool registerAutomatically = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await DependentProducerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
        /// Whether to automatically register a schema that matches <typeparamref name="TKey" />
        /// if one does not already exist.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            bool registerAutomatically = false
        ) => DependentProducerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, registerAutomatically));


        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize keys.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await DependentProducerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await DependentProducerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version
        ) => DependentProducerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, version));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            bool registerAutomatically = false,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) => DependentProducerBuilder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(
            registryClient,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static DependentProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            bool registerAutomatically = false,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) => DependentProducerBuilder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(
            registryConfiguration,
            registerAutomatically: registerAutomatically,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await DependentProducerBuilder.SetAvroValueSerializer(serializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            int id
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await DependentProducerBuilder.SetAvroValueSerializer(serializerBuilder, id);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder.
        /// </param>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize values.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            int id
        ) => DependentProducerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(id));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register a schema that matches <typeparamref name="TValue" />
        /// if one does not already exist.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            bool registerAutomatically = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await DependentProducerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
        /// Whether to automatically register a schema that matches <typeparamref name="TValue" />
        /// if one does not already exist.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            bool registerAutomatically = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await DependentProducerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
        /// Whether to automatically register a schema that matches <typeparamref name="TValue" />
        /// if one does not already exist.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            bool registerAutomatically = false
        ) => DependentProducerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, registerAutomatically));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
        /// The <see cref="DependentProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
        /// </param>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize values.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        public static async Task<DependentProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await DependentProducerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await DependentProducerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version);
            }
        }

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="DependentProducerBuilder">
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
            this DependentProducerBuilder<TKey, TValue> DependentProducerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            int version
        ) => DependentProducerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, version));
    }
}
