using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// A collection of <see cref="ProducerBuilder{TKey, TValue}" /> convenience methods that
    /// configure Avro serializers.
    /// </summary>
    public static class ProducerBuilderExtensions
    {
        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) => producerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryClient,
            registerAutomatically: registerAutomatically,
            resolveReferenceTypesAsNullable: resolveReferenceTypesAsNullable,
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
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false,
            Func<SerializationContext, string> subjectNameBuilder = null
        ) => producerBuilder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(
            registryConfiguration,
            registerAutomatically: registerAutomatically,
            resolveReferenceTypesAsNullable: resolveReferenceTypesAsNullable,
            subjectNameBuilder: subjectNameBuilder
        ));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
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
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, id);
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
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, id);
            }
        }

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
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(id));

        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically);
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
        /// Whether to automatically register a schema that matches <typeparamref name="TKey" />
        /// if one does not already exist.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, registerAutomatically);
            }
        }

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
        /// Whether to automatically register a schema that matches <typeparamref name="TKey" />
        /// if one does not already exist.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, registerAutomatically));


        /// <summary>
        /// Set the message key serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroKeySerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version);
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
                return await producerBuilder.SetAvroKeySerializer(serializerBuilder, subject, version);
            }
        }

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
        ) => producerBuilder.SetKeySerializer(await serializerBuilder.Build<TKey>(subject, version));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
        /// </param>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. The client should only be disposed
        /// after the producer; the serializer will use it to request schemas as messages are being
        /// produced.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false,
            Func<SerializationContext, string> subjectNameBuilder = null
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
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="registerAutomatically">
        /// Whether to automatically register schemas that match the type being serialized.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="subjectNameBuilder">
        /// A function that determines the subject name given the topic name and a component type
        /// (key or value). If none is provided, the default "{topic name}-{component}" naming
        /// convention will be used.
        /// </param>
        public static ProducerBuilder<TKey, TValue> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false,
            Func<SerializationContext, string> subjectNameBuilder = null
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
        /// <param name="registryClient">
        /// A client to use to resolve the schema. (The client will not be disposed.)
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
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, id);
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
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, id);
            }
        }

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
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(id));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically);
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
        /// Whether to automatically register a schema that matches <typeparamref name="TValue" />
        /// if one does not already exist.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            string subject,
            bool registerAutomatically = false,
            bool resolveReferenceTypesAsNullable = false
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryConfiguration))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, registerAutomatically);
            }
        }

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
        /// Whether to automatically register a schema that matches <typeparamref name="TValue" />
        /// if one does not already exist.
        /// </param>
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            SchemaRegistrySerializerBuilder serializerBuilder,
            string subject,
            bool registerAutomatically = false
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, registerAutomatically));

        /// <summary>
        /// Set the message value serializer.
        /// </summary>
        /// <param name="producerBuilder">
        /// The <see cref="ProducerBuilder{TKey, TValue}" /> instance to be configured.
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
        public static async Task<ProducerBuilder<TKey, TValue>> SetAvroValueSerializer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> producerBuilder,
            ISchemaRegistryClient registryClient,
            string subject,
            int version
        ) {
            using (var serializerBuilder = new SchemaRegistrySerializerBuilder(registryClient))
            {
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version);
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
                return await producerBuilder.SetAvroValueSerializer(serializerBuilder, subject, version);
            }
        }

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
        ) => producerBuilder.SetValueSerializer(await serializerBuilder.Build<TValue>(subject, version));
    }
}
