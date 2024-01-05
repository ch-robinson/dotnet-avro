namespace Chr.Avro.Cli
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Chr.Avro.Codegen;
    using Chr.Avro.Representation;
    using CommandLine;
    using CommandLine.Text;

    [Verb("generate", HelpText = "Generate C# code for a schema from the Schema Registry or stdin.")]
    public class GenerateCodeVerb : Verb, ISchemaResolutionOptions
    {
        private const string ByIdSet = "ById";

        private const string BySubjectSet = "BySubject";

        [Usage(ApplicationAlias = "dotnet avro")]
        public static IEnumerable<Example> Examples => new List<Example>
        {
            new Example("Generate code for a schema by ID", new GenerateCodeVerb
            {
                RegistryUrl = "http://registry:8081",
                SchemaId = 120,
            }),
            new Example("Generate code for a schema by ID, connecting to the Schema Registry using basic auth", new GenerateCodeVerb
            {
                RegistryConfig = new[]
                {
                    "schema.registry.basic.auth.user.info=exampleuser:password",
                },
                RegistryUrl = "http://registry:8081",
                SchemaId = 120,
            }),
        };

        [Option('c', "registry-config", HelpText = "Configuration options to provide to the registry client (multiple space-separated key=value pairs accepted).")]
        public IEnumerable<string> RegistryConfig { get; set; }

        [Option('r', "registry-url", HelpText = "The URL of the schema registry.")]
        public string RegistryUrl { get; set; }

        [Option('i', "id", SetName = ByIdSet, HelpText = "If a subject/version is not specified, the ID of the schema.")]
        public int? SchemaId { get; set; }

        [Option('s', "subject", SetName = BySubjectSet, HelpText = "If an ID is not specified, the subject of the schema.")]
        public string SchemaSubject { get; set; }

        [Option("component-model-annotations", HelpText = "Whether to emit component model annotations for record and enum descriptions.")]
        public bool ComponentModelAnnotations { get; set; }

        [Option("nullable-references", HelpText = "Whether reference types selected for nullable record fields should be annotated as nullable.")]
        public bool NullableReferences { get; set; }

        [Option('v', "version", SetName = BySubjectSet, HelpText = "The version of the schema.")]
        public int? SchemaVersion { get; set; }

        protected override async Task Run()
        {
            var generator = new CSharpCodeGenerator(
                enableDescriptionAttributeForDocumentation: ComponentModelAnnotations,
                enableNullableReferenceTypes: NullableReferences);
            var reader = new JsonSchemaReader();
            var schema = reader.Read(await ((ISchemaResolutionOptions)this).ResolveSchema());

            try
            {
                Console.WriteLine(generator.WriteCompilationUnit(schema));
            }
            catch (Exception exception)
            {
                throw new ProgramException(message: exception.Message, inner: exception);
            }
        }
    }
}
