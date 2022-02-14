namespace Chr.Avro.Cli
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Chr.Avro.Abstract;
    using Chr.Avro.Representation;
    using CommandLine;
    using CommandLine.Text;

    [Verb("create", HelpText = "Create an Avro schema for a .NET type.")]
    public class CreateSchemaVerb : Verb, IClrTypeOptions
    {
        [Usage(ApplicationAlias = "dotnet avro")]
        public static IEnumerable<Example> Examples => new List<Example>
        {
            new Example("Create a schema for a built-in type", new CreateSchemaVerb
            {
                TypeName = "System.DateTime",
            }),
            new Example("Create a schema for a type in a compiled assembly", new CreateSchemaVerb
            {
                AssemblyNames = new[] { "./out/Example.Models.dll" },
                TypeName = "Example.Models.ExampleModel",
            }),
            new Example("Create a schema for a type in a compiled assembly with dependencies", new CreateSchemaVerb
            {
                AssemblyNames = new[] { "System.Text.Json", "./out/Example.Models.dll" },
                TypeName = "Example.Models.ExampleModel",
            }),
        };

        [Option('a', "assembly", HelpText = "The name of or path to an assembly to load (multiple space-separated values accepted).")]
        public IEnumerable<string> AssemblyNames { get; set; }

        [Option("enum-behavior", HelpText = "The type of schema that enum types should be represented by. Options are \"symbolic\" (generate an \"enum\" schema; the default behavior), \"integral\" (generate an \"int\" or \"long\" schema based on the underlying type; the behavior for all flag enums), and \"nominal\" (generate a \"string\" schema).")]
        public EnumBehavior EnumBehavior { get; set; }

        [Option("nullable-references", HelpText = "Which reference types should be represented with nullable union schemas. Options are \"annotated\" (use nullable annotations if available; the default behavior), \"none\", and \"all\".", Default = NullableReferenceTypeBehavior.Annotated)]
        public NullableReferenceTypeBehavior NullableReferences { get; set; }

        [Option("temporal-behavior", HelpText = "Whether timestamps should be represented with \"string\" schemas (ISO 8601) or \"long\" schemas (timestamp logical types). Options are \"iso8601\", \"epochmilliseconds\", and \"epochmicroseconds\".")]
        public TemporalBehavior TemporalBehavior { get; set; }

        [Option('t', "type", Required = true, HelpText = "The type to build a schema for.")]
        public string TypeName { get; set; }

        protected override Task Run()
        {
            var schema = CreateSchema();
            var writer = new JsonSchemaWriter();

            Console.WriteLine(writer.Write(schema));

            return Task.CompletedTask;
        }

        protected Schema CreateSchema()
        {
            var type = ((IClrTypeOptions)this).ResolveType();

            var builder = new SchemaBuilder(
                enumBehavior: EnumBehavior,
                nullableReferenceTypeBehavior: NullableReferences,
                temporalBehavior: TemporalBehavior);

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
}
