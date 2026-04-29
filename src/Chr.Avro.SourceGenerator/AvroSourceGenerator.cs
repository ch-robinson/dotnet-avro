namespace Chr.Avro;

using System.IO;
using Chr.Avro.Abstract;
using Chr.Avro.Codegen;
using Chr.Avro.Representation;
using Microsoft.CodeAnalysis;

[Generator]
internal class AvroSourceGenerator : IIncrementalGenerator
{
    /// <inheritdoc/>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var avroFiles = context
            .AdditionalTextsProvider
            .Where(static file => file.Path.EndsWith(".avsc"))
            .Select(static (file, ct) =>
            {
                var fileName = Path.GetFileNameWithoutExtension(file.Path);
                var fileContent = file.GetText(ct);

                return fileContent is null
                    ? null
                    : new AvroFile(fileName, fileContent.ToString());
            });

        context.RegisterSourceOutput(avroFiles, GenerateWithAvro);
    }

    private static void GenerateWithAvro(SourceProductionContext context, AvroFile? avroFile)
    {
        if (avroFile is null)
        {
            return;
        }

        var jsonSchemaReader = new JsonSchemaReader();
        if (jsonSchemaReader.Read(avroFile.Content) is not RecordSchema schema)
        {
            return;
        }

        var namespaceName = StringConverter.ConvertToNamespaceCase(schema.Namespace ?? string.Empty);
        var className = StringConverter.ConvertToPascalCase(schema.Name);
        var fields = schema.Fields;

        if (string.IsNullOrWhiteSpace(namespaceName) || fields.Count == 0)
        {
            return;
        }

        var fileName = $"{namespaceName}.{className}.g.cs";

        var generator = new CSharpCodeGenerator();
        var unit = generator.GenerateCompilationUnit(schema);

        var code = unit.NormalizeWhitespace().ToFullString();

        context.AddSource(fileName, code);
    }

    private record AvroFile
    {
        public AvroFile(string name, string content)
        {
            Name = name;
            Content = content;
        }

        public string Name { get; }

        public string Content { get; }
    }
}
