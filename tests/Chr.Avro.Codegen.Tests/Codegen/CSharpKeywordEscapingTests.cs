namespace Chr.Avro.Codegen.Tests.Codegen
{
    using System.Linq;
    using Chr.Avro.Abstract;
    using Xunit;

    public class CSharpKeywordEscapingTests
    {
        [Fact]
        public void GivenRecordFieldsNamedLikeCSharpKeywords_WhenGeneratingCode_ShouldCompileAndExposeMatchingProperties()
        {
            var schema = new RecordSchema("TestEvent", new[]
            {
                new RecordField("event", new StringSchema()),
                new RecordField("do", new BooleanSchema()),
                new RecordField("class", new IntSchema()),
            });

            var sourceCode = new CSharpCodeGenerator().WriteCompilationUnit(schema);
            var compiledTypes = CSharpCodeCompiler.GetTypesDefinedInSourceCode(sourceCode);
            var generatedType = compiledTypes.Single(t => t.Name == "TestEvent");

            Assert.NotNull(generatedType.GetProperty("event"));
            Assert.NotNull(generatedType.GetProperty("do"));
            Assert.NotNull(generatedType.GetProperty("class"));
        }

        [Fact]
        public void GivenRecordSchemaNamedLikeCSharpKeyword_WhenGeneratingCode_ShouldCompile()
        {
            var schema = new RecordSchema("event", new[]
            {
                new RecordField("id", new IntSchema()),
            });

            var sourceCode = new CSharpCodeGenerator().WriteCompilationUnit(schema);
            var compiledTypes = CSharpCodeCompiler.GetTypesDefinedInSourceCode(sourceCode);

            Assert.Contains(compiledTypes, t => t.Name == "event");
        }

        [Fact]
        public void GivenEnumSymbolsNamedLikeCSharpKeywords_WhenGeneratingCode_ShouldCompileAndExposeMatchingMembers()
        {
            var schema = new EnumSchema("Verb", new[] { "do", "class", "namespace" });

            var sourceCode = new CSharpCodeGenerator().WriteCompilationUnit(schema);
            var compiledTypes = CSharpCodeCompiler.GetTypesDefinedInSourceCode(sourceCode);
            var generatedType = compiledTypes.Single(t => t.Name == "Verb");

            var memberNames = generatedType.GetEnumNames();
            Assert.Contains("do", memberNames);
            Assert.Contains("class", memberNames);
            Assert.Contains("namespace", memberNames);
        }

        [Fact]
        public void GivenSchemaWithNamespaceSegmentNamedLikeCSharpKeyword_WhenGeneratingCode_ShouldCompile()
        {
            var schema = new RecordSchema("Payload", new[]
            {
                new RecordField("value", new StringSchema()),
            })
            {
                Namespace = "com.example.event",
            };

            var sourceCode = new CSharpCodeGenerator().WriteCompilationUnit(schema);
            var compiledTypes = CSharpCodeCompiler.GetTypesDefinedInSourceCode(sourceCode);

            Assert.Contains(compiledTypes, t => t.FullName == "com.example.@event.Payload" || t.FullName == "com.example.event.Payload");
        }

        [Fact]
        public void GivenEnumReferencedByRecordField_WhenEnumNameIsCSharpKeyword_ShouldCompile()
        {
            var enumSchema = new EnumSchema("class", new[] { "A", "B" });
            var recordSchema = new RecordSchema("Holder", new[]
            {
                new RecordField("category", enumSchema),
            });

            var sourceCode = new CSharpCodeGenerator().WriteCompilationUnit(recordSchema);
            var compiledTypes = CSharpCodeCompiler.GetTypesDefinedInSourceCode(sourceCode);

            Assert.Contains(compiledTypes, t => t.Name == "Holder");
            Assert.Contains(compiledTypes, t => t.Name == "class" && t.IsEnum);
        }
    }
}
