using CommandLine;
using CommandLine.Text;
using Chr.Avro.Abstract;
using Chr.Avro.Codegen;
using Chr.Avro.Representation;
using Chr.Avro.Resolution;
using Chr.Avro.Serialization;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace Chr.Avro.Cli
{
    public abstract class Verb
    {
        public async Task<int> Execute()
        {
            try
            {
                await Run();
            }
            catch (ProgramException e)
            {
                if (e.Message is var message && !string.IsNullOrEmpty(message))
                {
                    Console.Error.WriteLine(message);
                }

                return e.Code;
            }

            return 0;
        }

        protected abstract Task Run();
    }

    [Verb("create", HelpText = "Create an Avro schema for a .NET type.")]
    public class CreateSchemaVerb : Verb, IClrTypeOptions
    {
        [Option('a', "assembly", HelpText = "The name of or path to an assembly to load (multiple space-separated values accepted).")]
        public IEnumerable<string> AssemblyNames { get; set; }

        [Option("enums-as-integers", HelpText = "Whether enums should be represented with \"int\" or \"long\" schemas.")]
        public bool EnumsAsIntegers { get; set; }

        [Option("nullable-references", HelpText = "Whether reference types should be represented with nullable union schemas.")]
        public bool NullableReferences { get; set; }

        [Option("temporal-behavior", HelpText = "Whether timestamps should be represented with \"string\" schemas (ISO 8601) or \"long\" schemas (timestamp logical types). Options are iso8601, epochmilliseconds, and epochmicroseconds.")]
        public TemporalBehavior TemporalBehavior { get; set; }

        [Option('t', "type", Required = true, HelpText = "The type to build a schema for.")]
        public string TypeName { get; set; }

        [Usage(ApplicationAlias = "dotnet avro")]
        public static IEnumerable<Example> Examples => new List<Example>
        {
            new Example("Create a schema for a built-in type", new CreateSchemaVerb
            {
                TypeName = "System.DateTime"
            }),
            new Example("Create a schema for a type in a compiled assembly", new CreateSchemaVerb
            {
                AssemblyNames = new[] { "./out/Example.Models.dll" },
                TypeName = "Example.Models.ExampleModel"
            }),
            new Example("Create a schema for a type in a compiled assembly with dependencies", new CreateSchemaVerb
            {
                AssemblyNames = new[] { "System.Text.Json", "./out/Example.Models.dll" },
                TypeName = "Example.Models.ExampleModel"
            })
        };

        protected override Task Run()
        {
            var schema = CreateSchema();
            var writer = new JsonSchemaWriter();

            Console.WriteLine(writer.Write(schema));

            return Task.CompletedTask;
        }

        protected Schema CreateSchema()
        {
            var type = this.ResolveType();

            var resolver = new DataContractResolver(
                resolveReferenceTypesAsNullable: NullableReferences,
                resolveUnderlyingEnumTypes: EnumsAsIntegers
            );

            var builder = new SchemaBuilder(TemporalBehavior, resolver);

            try
            {
                return builder.BuildSchema(type);
            }
            catch (UnsupportedTypeException inner)
            {
                throw new ProgramException(message: $"Failed to create a schema for {type}: The type is not supported by the resolver.", inner: inner);
            }
        }
    }

    [Verb("generate", HelpText = "Generate C# code for a schema from the Schema Registry or stdin.")]
    public class GenerateCodeVerb : Verb, ISchemaResolutionOptions
    {
        [Usage(ApplicationAlias = "dotnet avro")]
        public static IEnumerable<Example> Examples => new List<Example>
        {
            new Example("Generate code for a schema by ID", new GenerateCodeVerb
            {
                RegistryUrl = "http://registry:8081",
                SchemaId = 120,
            }),
        };

        private const string ByIdSet = "ById";

        private const string BySubjectSet = "BySubject";

        [Option('c', "registry-config", HelpText = "Configuration options to provide to the registry client (multiple space-separated key=value pairs accepted).")]
        public IEnumerable<string> RegistryConfig { get; set; }

        [Option('r', "registry-url", HelpText = "The URL of the schema registry.")]
        public string RegistryUrl { get; set; }

        [Option('i', "id", SetName = ByIdSet, HelpText = "If a subject/version is not specified, the ID of the schema.")]
        public int? SchemaId { get; set; }

        [Option('s', "subject", SetName = BySubjectSet, HelpText = "If an ID is not specified, the subject of the schema.")]
        public string SchemaSubject { get; set; }

        [Option('v', "version", SetName = BySubjectSet, HelpText = "The version of the schema.")]
        public int? SchemaVersion { get; set; }

        protected override async Task Run()
        {
            var generator = new CSharpCodeGenerator();
            var reader = new JsonSchemaReader();
            var schema = reader.Read(await this.ResolveSchema());

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

    [Verb("registry-get", HelpText = "Retrieve a schema from the Schema Registry.")]
    public class GetSchemaVerb : Verb, ISchemaResolutionOptions
    {
        [Usage(ApplicationAlias = "dotnet avro")]
        public static IEnumerable<Example> Examples => new List<Example>
        {
            new Example("Get a schema by ID", new GetSchemaVerb
            {
                RegistryUrl = "http://registry:8081",
                SchemaId = 120,
            }),
        };

        private const string ByIdSet = "ById";

        private const string BySubjectSet = "BySubject";

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
            Console.WriteLine(await this.ResolveSchema());
        }
    }

    [Verb("registry-test", HelpText = "Verify that a .NET type is compatible with a schema in the Schema Registry.")]
    public class TestSchemaVerb : Verb, IClrTypeOptions, ISchemaResolutionOptions
    {
        [Usage(ApplicationAlias = "dotnet avro")]
        public static IEnumerable<Example> Examples => new List<Example>
        {
            new Example("Test that a type works with the latest version of a subject", new TestSchemaVerb
            {
                AssemblyNames = new[] { "./out/Example.Models.dll" },
                RegistryUrl = "http://registry:8081",
                SchemaSubject = "example_subject",
                TypeName = "Example.Models.ExampleModel",
            }),
        };

        private const string ByIdSet = "ById";

        private const string BySubjectSet = "BySubject";

        [Option('a', "assembly", HelpText = "The name of or path to an assembly to load (multiple space-separated values accepted).")]
        public IEnumerable<string> AssemblyNames { get; set; }

        [Option('t', "type", Required = true, HelpText = "The type to test.")]
        public string TypeName { get; set; }

        [Option('c', "registry-config", HelpText = "Configuration options to provide to the registry client (multiple space-separated key=value pairs accepted).")]
        public IEnumerable<string> RegistryConfig { get; set; }

        [Option('r', "registry-url", Required = true, HelpText = "The URL of the schema registry.")]
        public string RegistryUrl { get; set; }

        [Option('i', "id", Required = true, SetName = ByIdSet, HelpText = "If a subject/version is not specified, the ID of the target schema.")]
        public int? SchemaId { get; set; }

        [Option('s', "subject", Required = true, SetName = BySubjectSet, HelpText = "If an ID is not specified, the subject of the target schema.")]
        public string SchemaSubject { get; set; }

        [Option('v', "version", SetName = BySubjectSet, HelpText = "The version of the target schema.")]
        public int? SchemaVersion { get; set; }

        protected override async Task Run()
        {
            var type = this.ResolveType();

            var reader = new JsonSchemaReader();
            var schema = reader.Read(await this.ResolveSchema());

            try
            {
                var builder = new BinaryDeserializerBuilder();
                var method = typeof(IBinaryDeserializerBuilder)
                    .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDeserializer))
                    .MakeGenericMethod(type);

                method.Invoke(builder, new[] { schema });
            }
            catch (TargetInvocationException exception) when (exception.InnerException is Exception inner)
            {
                throw new ProgramException(message: $"A deserializer cannot be created for {type}: {inner.Message}", inner: inner);
            }

            try
            {
                var builder = new BinarySerializerBuilder();
                var method = typeof(IBinarySerializerBuilder)
                    .GetMethod(nameof(IBinarySerializerBuilder.BuildSerializer))
                    .MakeGenericMethod(type);

                method.Invoke(builder, new[] { schema });
            }
            catch (TargetInvocationException exception) when (exception.InnerException is Exception inner)
            {
                throw new ProgramException(message: $"A serializer cannot be created for {type}: {inner.Message}", inner: inner);
            }

            Console.Error.WriteLine($"{type} is compatible with the schema.");
        }
    }
}
