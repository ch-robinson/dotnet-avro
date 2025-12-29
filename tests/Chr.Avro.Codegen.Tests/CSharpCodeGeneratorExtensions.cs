using System.IO;
using System.Text;
using Chr.Avro.Abstract;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Formatting;

namespace Chr.Avro.Codegen.Tests;

public static class CSharpCodeGeneratorExtensions
{
    public static string WriteCompilationUnit(this CSharpCodeGenerator generator, Schema schema)
    {
        var stream = new MemoryStream();

        using (stream)
        {
            using var workspace = new AdhocWorkspace();
            using var writer = new StreamWriter(stream);

            var unit = generator.GenerateCompilationUnit(schema) as SyntaxNode;
            unit = Formatter.Format(unit, workspace);

            unit.WriteTo(writer);
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
