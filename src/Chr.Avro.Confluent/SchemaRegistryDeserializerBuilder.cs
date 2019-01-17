using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Chr.Avro.Confluent
{
    /// <summary>
    /// Builds <see cref="Deserializer{T}" /> delegates that rely on schemas from a Schema Registry
    /// instance.
    /// </summary>
    public interface ISchemaRegistryDeserializerBuilder
    {
        /// <summary>
        /// Builds a deserializer that resolves schemas on the fly.
        /// </summary>
        Deserializer<T> BuildDeserializer<T>();

        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <param name="id">
        /// The ID of the schema used to deserialize data.
        /// </param>
        Task<Deserializer<T>> BuildDeserializer<T>(int id);

        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to deserialize data. The latest
        /// version of the subject will be resolved.
        /// </param>
        Task<Deserializer<T>> BuildDeserializer<T>(string subject);

        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to deserialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        Task<Deserializer<T>> BuildDeserializer<T>(string subject, int version);
    }

    /// <summary>
    /// Builds <see cref="T:Confluent.Kafka.Deserializer{T}" /> delegates that
    /// rely on schemas from a Schema Registry instance.
    /// </summary>
    public class SchemaRegistryDeserializerBuilder : ISchemaRegistryDeserializerBuilder, IDisposable
    {
        private readonly IBinaryDeserializerBuilder _builder;

        private readonly bool _disposeRegistry;

        private readonly IJsonSchemaReader _reader;

        /// <summary>
        /// The client to use for Schema Registry operations.
        /// </summary>
        protected readonly ISchemaRegistryClient Registry;

        /// <summary>
        /// Creates a deserializer builder.
        /// </summary>
        /// <param name="configuration">
        /// Schema Registry client configuration.
        /// </param>
        /// <param name="builder">
        /// A deserializer builder instance to use for delegate construction. If none is provided,
        /// the default <see cref="BinaryDeserializerBuilder" /> will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the configuration is null.
        /// </exception>
        public SchemaRegistryDeserializerBuilder(IEnumerable<KeyValuePair<string, string>> configuration, IBinaryDeserializerBuilder builder = null)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            Registry = new CachedSchemaRegistryClient(configuration);

            _builder = builder ?? new BinaryDeserializerBuilder();
            _disposeRegistry = true;
            _reader = new JsonSchemaReader();
        }

        /// <summary>
        /// Creates a deserializer builder.
        /// </summary>
        /// <param name="registry">
        /// A client to use for Schema Registry operations. (The client will
        /// not be disposed.)
        /// </param>
        /// <param name="builder">
        /// A deserializer builder instance to use for delegate construction. If none is provided,
        /// the default <see cref="BinaryDeserializerBuilder" /> will be used.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the registry client is null.
        /// </exception>
        public SchemaRegistryDeserializerBuilder(ISchemaRegistryClient registry, IBinaryDeserializerBuilder builder = null)
        {
            Registry = registry ?? throw new ArgumentNullException(nameof(registry));

            _builder = builder ?? new BinaryDeserializerBuilder();
            _disposeRegistry = false;
            _reader = new JsonSchemaReader();
        }

        /// <summary>
        /// Builds a deserializer that resolves schemas on the fly.
        /// </summary>
        /// <remarks>
        /// The deserializer expects data to conform to the Confluent wire
        /// format and retrieves schemas as needed from the registry.
        /// </remarks>
        public Deserializer<T> BuildDeserializer<T>()
        {
            var cache = new ConcurrentDictionary<int, Func<Stream, T>>();

            return (topic, data, isNull) =>
            {
                using (var stream = new MemoryStream(data.ToArray(), false))
                {
                    if (stream.ReadByte() != 0x00)
                    {
                        throw new InvalidDataException("Data does not conform to the Confluent wire format.");
                    }

                    var bytes = new byte[4];
                    stream.Read(bytes, 0, 4);

                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(bytes);
                    }

                    var deserialize = cache.GetOrAdd(BitConverter.ToInt32(bytes, 0), id =>
                    {
                        var json = Registry.GetSchemaAsync(id)
                            .ConfigureAwait(continueOnCapturedContext: false)
                            .GetAwaiter()
                            .GetResult();

                        return _builder.BuildDelegate<T>(_reader.Read(json));
                    });

                    return deserialize(stream);
                }
            };
        }

        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <param name="id">
        /// The ID of the schema used to deserialize data.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Deserializer<T>> BuildDeserializer<T>(int id)
        {
            return BuildDeserializer<T>(id, await Registry.GetSchemaAsync(id));
        }


        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to deserialize data. The latest
        /// version of the subject will be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Deserializer<T>> BuildDeserializer<T>(string subject)
        {
            var schema = await Registry.GetLatestSchemaAsync(subject);

            return BuildDeserializer<T>(schema.Id, schema.SchemaString);
        }

        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <param name="subject">
        /// The subject of the schema used to deserialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is incompatible with the retrieved schema.
        /// </exception>
        public async Task<Deserializer<T>> BuildDeserializer<T>(string subject, int version)
        {
            var schema = await Registry.GetSchemaAsync(subject, version);
            var id = await Registry.GetSchemaIdAsync(subject, schema);

            return BuildDeserializer<T>(id, schema);
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

        private Deserializer<T> BuildDeserializer<T>(int id, string schema)
        {
            var deserialize = _builder.BuildDelegate<T>(_reader.Read(schema));

            return (topic, data, isNull) =>
            {
                using (var stream = new MemoryStream(data.ToArray(), false))
                {
                    if (stream.ReadByte() != 0x00)
                    {
                        throw new InvalidDataException("Data does not conform to the Confluent wire format.");
                    }

                    var bytes = new byte[4];
                    stream.Read(bytes, 0, 4);

                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(bytes);
                    }

                    var received = BitConverter.ToInt32(bytes, 0);
                    
                    if (received != id)
                    {
                        throw new InvalidDataException($"The received schema ({received}) does not match the specified schema ({id}).");
                    }

                    return deserialize(stream);
                }
            };
        }
    }
}
