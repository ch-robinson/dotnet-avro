namespace Chr.Avro.Codegen.Tests
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.Loader;
    using System.Text;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using Microsoft.CodeAnalysis;
    using Microsoft.CodeAnalysis.CSharp;
    using Microsoft.CodeAnalysis.Emit;

    public class TestHelper
    {
        internal static IEnumerable<System.Reflection.TypeInfo> GetTypesGeneratedFromSchema<T>(Schema schema, bool enableNullableReferenceTypes = true)
        {
            var sourceCode = new CSharpCodeGenerator(enableNullableReferenceTypes).WriteCompilationUnit(schema);
            return CompileAssembly(sourceCode).DefinedTypes;
        }

        internal static void AssertCanDeserializeTypeFromSchema(System.Reflection.TypeInfo type, Schema schema)
        {
            var context = new BinaryDeserializerBuilderContext();

            var root = new BinaryDeserializerBuilder().BuildExpression(type, schema, context);

            // Compile() will throw if not compatible
            Expression.Lambda(
                Expression.Block(
                context.Assignments
                    .Select(a => a.Key),
                context.Assignments
                    .Select(a => (Expression)Expression.Assign(a.Key, a.Value))
                    .Concat(new[] { root })),
                new[] { context.Reader }).Compile();
        }

        // Based on https://github.com/joelmartinez/dotnet-core-roslyn-sample/blob/master/Program.cs
        private static Assembly CompileAssembly(string codeToCompile)
        {
            SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(codeToCompile);
            string assemblyName = Path.GetRandomFileName();

            var refPaths = new[]
            {
                typeof(DescriptionAttribute).Assembly.Location, // Load the DescriptionAttribute
                typeof(object).GetTypeInfo().Assembly.Location,
                typeof(Console).GetTypeInfo().Assembly.Location,
                Path.Combine(Path.GetDirectoryName(typeof(System.Runtime.GCSettings).GetTypeInfo().Assembly.Location), "System.Runtime.dll"),
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
                    return AssemblyLoadContext.Default.LoadFromStream(ms);
                }
            }
        }
    }
}
