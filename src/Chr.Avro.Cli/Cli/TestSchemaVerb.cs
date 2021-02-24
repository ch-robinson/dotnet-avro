namespace Chr.Avro.Cli
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading.Tasks;
    using Chr.Avro.Representation;
    using Chr.Avro.Serialization;
    using CommandLine;
    using CommandLine.Text;

    [Verb("registry-test", HelpText = "Verify that a .NET type is compatible with a schema in the Schema Registry.")]
    public class TestSchemaVerb : Verb, IClrTypeOptions, ISchemaResolutionOptions
    {
        private const string ByIdSet = "ById";

        private const string BySubjectSet = "BySubject";

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

        [Option('a', "assembly", HelpText = "The name of or path to an assembly to load (multiple space-separated values accepted).")]
        public IEnumerable<string> AssemblyNames { get; set; }

        [Option('t', "type", Required = true, HelpText = "The type to test.")]
        public string TypeName { get; set; }

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
            var type = ((IClrTypeOptions)this).ResolveType();

            var reader = new JsonSchemaReader();
            var schema = reader.Read(await ((ISchemaResolutionOptions)this).ResolveSchema());

            try
            {
                var builder = new BinaryDeserializerBuilder();
                var method = typeof(BinaryDeserializerBuilder)
                    .GetMethod(nameof(BinaryDeserializerBuilder.BuildDelegate))
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
                    .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate))
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
