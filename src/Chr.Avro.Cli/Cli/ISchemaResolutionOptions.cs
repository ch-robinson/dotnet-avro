namespace Chr.Avro.Cli
{
    using System;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using SchemaRegistryClient = global::Confluent.SchemaRegistry.CachedSchemaRegistryClient;
    using SchemaRegistryConfiguration = global::Confluent.SchemaRegistry.SchemaRegistryConfig;
    using SchemaRegistryException = global::Confluent.SchemaRegistry.SchemaRegistryException;
    using SchemaType = global::Confluent.SchemaRegistry.SchemaType;

    public interface ISchemaResolutionOptions
    {
        IEnumerable<string> RegistryConfig { get; set; }

        string RegistryUrl { get; }

        int? SchemaId { get; }

        string SchemaSubject { get; }

        int? SchemaVersion { get; }

        public async Task<string> ResolveSchema()
        {
            if (Console.IsInputRedirected)
            {
                using var stream = Console.OpenStandardInput();
                using var streamReader = new System.IO.StreamReader(stream);

                return await streamReader.ReadToEndAsync();
            }
            else
            {
                var configuration = new SchemaRegistryConfiguration();

                foreach (var entry in RegistryConfig)
                {
                    var match = Regex.Match(entry, @"^(?<key>.+?)=(?<value>.+)$");

                    if (!match.Success)
                    {
                        throw new ProgramException(message: "Registry configuration options should be specified as key=value pairs.");
                    }

                    configuration.Set(match.Groups["key"].Value, match.Groups["value"].Value);
                }

                if (string.IsNullOrWhiteSpace(RegistryUrl))
                {
                    if (string.IsNullOrWhiteSpace(configuration.Url))
                    {
                        throw new ProgramException(message: "When not reading from stdin, you must provide --registry-url.");
                    }
                }
                else
                {
                    configuration.Url = RegistryUrl;
                }

                using var client = new SchemaRegistryClient(configuration);

                try
                {
                    if (SchemaId is int id)
                    {
                        if (!string.IsNullOrEmpty(SchemaSubject) || SchemaVersion.HasValue)
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
                        if (SchemaSubject is var subject && string.IsNullOrEmpty(subject))
                        {
                            throw new ProgramException(message: "Either --id or --subject (and optionally --version) must be provided.");
                        }

                        if (SchemaVersion is int version)
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
                                var schema = await client.GetLatestSchemaAsync(SchemaSubject);

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
