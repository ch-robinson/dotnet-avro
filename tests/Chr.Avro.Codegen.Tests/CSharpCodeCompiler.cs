namespace Chr.Avro.Codegen.Tests
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using Microsoft.CodeAnalysis;
    using Microsoft.CodeAnalysis.CSharp;
    using Microsoft.CodeAnalysis.Emit;

    public class CSharpCodeCompiler
    {
        internal static IEnumerable<System.Reflection.TypeInfo> GetTypesDefinedInSourceCode(string sourceCode)
        {
            return CompileAssembly(sourceCode).DefinedTypes;
        }

        // Based on https://github.com/joelmartinez/dotnet-core-roslyn-sample/blob/master/Program.cs
        private static Assembly CompileAssembly(string codeToCompile)
        {
            SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(codeToCompile);
            var assemblyName = Path.GetRandomFileName();
            var runtimePath = Path.GetDirectoryName(typeof(object).GetTypeInfo().Assembly.Location);

            var refPaths = new[]
            {
#if !NET6_0_OR_GREATER
                Path.Combine(runtimePath, "netstandard.dll"),
#endif
                typeof(object).GetTypeInfo().Assembly.Location, // mscorlib
                typeof(DescriptionAttribute).Assembly.Location, // System.ComponentModel.Primitives
                Path.Combine(runtimePath, "System.Runtime.dll"), // System.Runtime
                typeof(Schema).Assembly.Location, // Chr.Avro
                typeof(BinaryDeserializerBuilder).Assembly.Location, // Chr.Avro.Binary
            };
            MetadataReference[] references = refPaths.Select(r => MetadataReference.CreateFromFile(r)).ToArray();

            CSharpCompilation compilation = CSharpCompilation.Create(
                assemblyName,
                syntaxTrees: new[] { syntaxTree },
                references: references,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

            using (var ms = new MemoryStream())
            {
                EmitResult result = compilation.Emit(ms);

                if (!result.Success)
                {
                    IEnumerable<Diagnostic> failures = result.Diagnostics.Where(diagnostic =>
                        diagnostic.IsWarningAsError ||
                        diagnostic.Severity == DiagnosticSeverity.Error);

                    var stringBuilder = new StringBuilder();
                    foreach (Diagnostic diagnostic in failures)
                    {
                        stringBuilder.AppendLine($"\t{diagnostic.Id}: {diagnostic.GetMessage()}");
                    }

                    throw new Exception(stringBuilder.ToString());
                }
                else
                {
                    ms.Seek(0, SeekOrigin.Begin);

                    // Just load the assembly bytes, since System.Runtime.Loader is not available in net462
                    // https://github.com/dotnet/runtime/issues/22732
                    return Assembly.Load(ms.ToArray());
                }
            }
        }
    }
}
