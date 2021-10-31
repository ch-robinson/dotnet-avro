namespace Chr.Avro.Cli
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using CommandLine;
    using CommandLine.Text;

    [Verb("registry-get", HelpText = "Retrieve a schema from the Schema Registry.")]
    public class GetSchemaVerb : Verb, ISchemaResolutionOptions
    {
        private const string ByIdSet = "ById";

        private const string BySubjectSet = "BySubject";

        [Usage(ApplicationAlias = "dotnet avro")]
        public static IEnumerable<Example> Examples => new List<Example>
        {
            new Example("Get a schema by ID", new GetSchemaVerb
            {
                RegistryUrl = "http://registry:8081",
                SchemaId = 120,
            }),
        };

        [Option('c', "registry-config", HelpText = "Configuration options to provide to the registry client (multiple space-separated key=value pairs accepted).")]
        public IEnumerable<string> RegistryConfig { get; set; }

        [Option('r', "registry-url", Required = true, HelpText = "The URL of the schema registry.")]
        public string RegistryUrl { get; set; }

        [Option('i', "id", Required = true, SetName = ByIdSet, HelpText = "If a subject/version is not specified, the ID of the schema.")]
        public int? SchemaId { get; set; }

        [Option('s', "subject", Required = true, SetName = BySubjectSet, HelpText = "If an ID is not specified, the subject of the schema.")]
        public string SchemaSubject { get; set; }

        [Option('v', "version", SetName = BySubjectSet, HelpText = "The version of the schema.")]
        public int? SchemaVersion { get; set; }

        protected override async Task Run()
        {
            Console.WriteLine(await ((ISchemaResolutionOptions)this).ResolveSchema());
        }
    }
}
