using System;
using System.Threading.Tasks;

using SchemaRegistryClient = Confluent.SchemaRegistry.CachedSchemaRegistryClient;
using SchemaRegistryConfiguration = Confluent.SchemaRegistry.SchemaRegistryConfig;
using SchemaRegistryException = Confluent.SchemaRegistry.SchemaRegistryException;

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
                    throw new ProgramException(message: "When not using stdin, you must use --registry-url");
                }

                var configuration = new SchemaRegistryConfiguration
                {
                    SchemaRegistryUrl = options.RegistryUrl
                };

                using var client = new SchemaRegistryClient(configuration);

                try
                {
                    if (options.SchemaId is int id)
                    {
                        if (!string.IsNullOrEmpty(options.SchemaSubject) || options.SchemaVersion.HasValue)
                        {
                            throw new ProgramException(message: "When using --id, don’t use --schema or --version.");
                        }

                        try
                        {
                            return await client.GetSchemaAsync(id);
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
                            throw new ProgramException(message: "Either --id or --schema (and optionally --version) must be provided.");
                        }

                        if (options.SchemaVersion is int version)
                        {
                            try
                            {
                                return await client.GetSchemaAsync(subject, version);
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
                                return (await client.GetLatestSchemaAsync(options.SchemaSubject)).SchemaString;
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
