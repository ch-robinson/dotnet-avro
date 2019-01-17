using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// Builds <see cref="Serializer{T}" /> delegates that rely on schemas from a Schema Registry
    /// instance.
    /// </summary>
    public interface ISchemaRegistrySerializerBuilder
    {
        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="id">
        /// The ID of the schema used to serialize data.
        /// </param>
        Task<Serializer<T>> BuildSerializer<T>(int id);

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to serialize data. The latest version of the subject
        /// will be resolved.
        /// </param>
        Task<Serializer<T>> BuildSerializer<T>(string subject);

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to serialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        Task<Serializer<T>> BuildSerializer<T>(string subject, int version);
    }

    /// <summary>
    /// Builds <see cref="Serializer{T}" /> delegates that rely on schemas from a Schema Registry
    /// instance.
    /// </summary>
    public class SchemaRegistrySerializerBuilder : ISchemaRegistrySerializerBuilder, IDisposable
    {
        private readonly IBinarySerializerBuilder _builder;

        private readonly bool _disposeRegistry;

        private readonly IJsonSchemaReader _reader;

        /// <summary>
        /// The client to use for Schema Registry operations.
        /// </summary>
        protected readonly ISchemaRegistryClient Registry;

        /// <summary>
        /// Creates a serializer builder.
        /// </summary>
        /// <param name="configuration">
        /// Schema Registry client configuration.
        /// </param>
        /// <param name="builder">
        /// A serializer builder instance to use for delegate construction. If none is provided, the
        /// default <see cref="BinarySerializerBuilder" /> will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the configuration is null.
        /// </exception>
        public SchemaRegistrySerializerBuilder(IEnumerable<KeyValuePair<string, string>> configuration, IBinarySerializerBuilder builder = null)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            Registry = new CachedSchemaRegistryClient(configuration);

            _builder = builder ?? new BinarySerializerBuilder();
            _disposeRegistry = true;
            _reader = new JsonSchemaReader();
        }

        /// <summary>
        /// Creates a serializer builder.
        /// </summary>
        /// <param name="registry">
        /// A client to use for Schema Registry operations. (The client will
        /// not be disposed.)
        /// </param>
        /// <param name="builder">
        /// A serializer builder instance to use for delegate construction. If none is provided, the
        /// default <see cref="BinarySerializerBuilder" /> will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public SchemaRegistrySerializerBuilder(ISchemaRegistryClient registry, IBinarySerializerBuilder builder = null)
        {
            Registry = registry ?? throw new ArgumentNullException(nameof(registry));

            _builder = builder ?? new BinarySerializerBuilder();
            _disposeRegistry = false;
            _reader = new JsonSchemaReader();
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="id">
        /// The ID of the schema used to serialize data.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Serializer<T>> BuildSerializer<T>(int id)
        {
            return BuildSerializer<T>(id, await Registry.GetSchemaAsync(id));
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to serialize data. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Serializer<T>> BuildSerializer<T>(string subject)
        {
            var schema = await Registry.GetLatestSchemaAsync(subject);

            return BuildSerializer<T>(schema.Id, schema.SchemaString);
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to serialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Serializer<T>> BuildSerializer<T>(string subject, int version)
        {
            var schema = await Registry.GetSchemaAsync(subject, version);
            var id = await Registry.GetSchemaIdAsync(subject, schema);

            return BuildSerializer<T>(id, schema);
        }

        /// <summary>
        /// Disposes the builder, freeing up any resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposeRegistry)
            {
                Registry?.Dispose();
            }
        }

        private Serializer<T> BuildSerializer<T>(int id, string schema)
        {
            var bytes = BitConverter.GetBytes(id);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            var serialize = _builder.BuildDelegate<T>(_reader.Read(schema));

            return (topic, data) =>
            {
                var stream = new MemoryStream();

                using (stream)
                {
                    stream.WriteByte(0x00);
                    stream.Write(bytes, 0, 4);

                    serialize(data, stream);
                }

                return stream.ToArray();
            };
        }
    }
}
