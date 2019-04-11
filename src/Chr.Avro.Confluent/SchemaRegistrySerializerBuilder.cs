using Chr.Avro.Representation;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// Builds <see cref="T:Serializer{T}" /> delegates that are locked into specific schemas from
    /// a Schema Registry instance.
    /// </summary>
    public class SchemaRegistrySerializerBuilder : IDisposable
    {
        private readonly bool _disposeRegistryClient;

        private readonly IJsonSchemaReader _schemaReader;

        private readonly Serialization.IBinarySerializerBuilder _serializerBuilder;

        /// <summary>
        /// The client to use for Schema Registry operations.
        /// </summary>
        protected readonly ISchemaRegistryClient RegistryClient;

        /// <summary>
        /// Creates a serializer builder.
        /// </summary>
        /// <param name="registryConfiguration">
        /// Schema Registry configuration. Using the <see cref="SchemaRegistryConfig" /> class is
        /// highly recommended.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder (used to build serialization functions for C# types). If none is
        /// provided, the default serializer builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        public SchemaRegistrySerializerBuilder(
            IEnumerable<KeyValuePair<string, string>> registryConfiguration,
            Serialization.IBinarySerializerBuilder serializerBuilder = null,
            IJsonSchemaReader schemaReader = null
        ) : this(
            new CachedSchemaRegistryClient(registryConfiguration),
            serializerBuilder,
            schemaReader
        ) {
            _disposeRegistryClient = true;
        }

        /// <summary>
        /// Creates a serializer builder.
        /// </summary>
        /// <param name="registryClient">
        /// A client to use for Schema Registry operations. (The client will not be disposed.)
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder (used to build serialization functions for C# types). If none is
        /// provided, the default serializer builder will be used.
        /// </param>
        /// <param name="schemaReader">
        /// A JSON schema reader (used to convert schemas received from the registry into abstract
        /// representations). If none is provided, the default schema reader will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public SchemaRegistrySerializerBuilder(
            ISchemaRegistryClient registryClient,
            Serialization.IBinarySerializerBuilder serializerBuilder = null,
            IJsonSchemaReader schemaReader = null
        ) {
            RegistryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));

            _disposeRegistryClient = false;
            _schemaReader = schemaReader ?? new JsonSchemaReader();
            _serializerBuilder = serializerBuilder ?? new Serialization.BinarySerializerBuilder();
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize data.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Serializer<T>> BuildSerializer<T>(int id)
        {
            return BuildSerializer<T>(id, await RegistryClient.GetSchemaAsync(id));
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Serializer<T>> BuildSerializer<T>(string subject)
        {
            var schema = await RegistryClient.GetLatestSchemaAsync(subject);

            return BuildSerializer<T>(schema.Id, schema.SchemaString);
        }

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Serializer<T>> BuildSerializer<T>(string subject, int version)
        {
            var schema = await RegistryClient.GetSchemaAsync(subject, version);
            var id = await RegistryClient.GetSchemaIdAsync(subject, schema);

            return BuildSerializer<T>(id, schema);
        }

        /// <summary>
        /// Disposes the builder, freeing up any resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposeRegistryClient)
            {
                RegistryClient?.Dispose();
            }
        }

        private Serializer<T> BuildSerializer<T>(int id, string schema)
        {
            var bytes = BitConverter.GetBytes(id);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            var serialize = _serializerBuilder.BuildDelegate<T>(_schemaReader.Read(schema));

            return data =>
            {
                var stream = new MemoryStream();

                using (stream)
                {
                    stream.WriteByte(0x00);
                    stream.Write(bytes, 0, bytes.Length);

                    serialize(data, stream);
                }

                return stream.ToArray();
            };
        }
    }
}
