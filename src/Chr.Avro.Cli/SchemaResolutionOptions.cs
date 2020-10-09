using System;
using System.Threading.Tasks;
using SchemaRegistryClient = Confluent.SchemaRegistry.CachedSchemaRegistryClient;
using SchemaRegistryConfiguration = Confluent.SchemaRegistry.SchemaRegistryConfig;
using SchemaRegistryException = Confluent.SchemaRegistry.SchemaRegistryException;
using SchemaType = Confluent.SchemaRegistry.SchemaType;

namespace Chr.Avro.Cli
{
    public interface ISchemaResolutionOptions
    {
        string RegistryUrl { get; }

        int? SchemaId { get; }

        string SchemaSubject { get; }

        int? SchemaVersion { get; }
    }

    internal static class SchemaResolutionOptionExtensions
    {
        public static async Task<string> ResolveSchema(this ISchemaResolutionOptions options)
        {
            if (Console.IsInputRedirected)
            {
                using var stream = Console.OpenStandardInput();
                using var streamReader = new System.IO.StreamReader(stream);

                return await streamReader.ReadToEndAsync();
            }
            else
            {
                if (string.IsNullOrWhiteSpace(options.RegistryUrl))
                {
                    throw new ProgramException(message: "When not reading from stdin, you must provide --registry-url.");
                }

                var configuration = new SchemaRegistryConfiguration
                {
                    Url = options.RegistryUrl
                };

                using var client = new SchemaRegistryClient(configuration);

                try
                {
                    if (options.SchemaId is int id)
                    {
                        if (!string.IsNullOrEmpty(options.SchemaSubject) || options.SchemaVersion.HasValue)
                        {
                            throw new ProgramException(message: "When using --id, don’t use --subject or --version.");
                        }

                        try
                        {
                            var schema = await client.GetSchemaAsync(id);

                            if (schema.SchemaType != SchemaType.Avro)
                            {
                                throw new ProgramException(message: $"The schema with ID {id} is not an Avro schema.");
                            }

                            return schema.SchemaString;
                        }
                        catch (AggregateException aggregate) when (aggregate.InnerException is SchemaRegistryException inner)
                        {
                            throw new ProgramException(message: $"Failed to retrieve schema with ID {id} ({inner.Message}).", inner: inner);
                        }
                    }
                    else
                    {
                        if (options.SchemaSubject is var subject && string.IsNullOrEmpty(subject))
                        {
                            throw new ProgramException(message: "Either --id or --subject (and optionally --version) must be provided.");
                        }

                        if (options.SchemaVersion is int version)
                        {
                            try
                            {
                                var schema = await client.GetRegisteredSchemaAsync(subject, version);

                                if (schema.SchemaType != SchemaType.Avro)
                                {
                                    throw new ProgramException(message: $"The schema with subject {subject} and version {version} is not an Avro schema.");
                                }

                                return schema.SchemaString;
                            }
                            catch (AggregateException aggregate) when (aggregate.InnerException is SchemaRegistryException inner)
                            {
                                throw new ProgramException(message: $"Failed to retrieve schema with subject {subject} and version {version} ({inner.Message}).", inner: inner);
                            }
                        }
                        else
                        {
                            try
                            {
                                var schema = await client.GetLatestSchemaAsync(options.SchemaSubject);

                                if (schema.SchemaType != SchemaType.Avro)
                                {
                                    throw new ProgramException(message: $"The latest schema with subject {subject} is not an Avro schema.");
                                }

                                return schema.SchemaString;
                            }
                            catch (AggregateException aggregate) when (aggregate.InnerException is SchemaRegistryException inner)
                            {
                                throw new ProgramException(message: $"Failed to retrieve latest schema with subject {subject} ({inner.Message}).", inner: inner);
                            }
                        }
                    }
                }
                catch (AggregateException aggregate) when (aggregate.InnerException is Exception inner)
                {
                    throw new ProgramException(message: $"Failed to connect to the Schema Registry ({inner.Message}).", inner: inner);
                }
            }
        }
    }
}
