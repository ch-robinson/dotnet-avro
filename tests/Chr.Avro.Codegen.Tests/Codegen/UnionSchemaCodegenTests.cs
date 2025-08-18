namespace Chr.Avro.Codegen.Tests.Codegen
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Chr.Avro.Serialization;
    using Xunit;

    public class UnionSchemaCodegenTests
    {
        [Fact]
        public void GivenPolymorphicField_WhenGeneratingSchema_ShouldContainUnionOfDerivedTypes()
        {
            // Arrange: build schema for Zoo
            List<Func<ISchemaBuilder, ISchemaBuilderCase>> builders = SchemaBuilder.CreateDefaultCaseBuilders().ToList();

            // Prepend our custom case to handle the polymorphic Animal type
            builders.Insert(0, builder => new PolymorphicClassAUnionSchemaBuilderCase(builder));
            builders.Insert(0, builder => new PolymorphicClassBUnionSchemaBuilderCase(builder));

            var schema = new SchemaBuilder(builders).BuildSchema<Polymorphic>();
            var codeGenerator = new CSharpCodeGenerator();
            var sourceCode = codeGenerator.WriteCompilationUnit(schema);
            var compiledTypes = CSharpCodeCompiler.GetTypesDefinedInSourceCode(sourceCode);
            var generatedType = compiledTypes.Single(x => x.Name == typeof(Polymorphic).Name);

            // Validate the generated type
            var abstractFieldA = generatedType.GetProperty("AbstractFieldA");
            Assert.NotNull(abstractFieldA);

            // Check that the generated types include original polymorphic derived classes
            var todoRename = Assert.Single(compiledTypes, t => t.Name == "ITodoRename" && t.IsInterface);
            Assert.Contains(compiledTypes, t => t.Name == "TodoRenameUnknown" && t.IsClass && t.ImplementedInterfaces.Contains(todoRename.AsType()));
            Assert.Contains(compiledTypes, t => t.Name == nameof(PolymorphicClassAA) && t.ImplementedInterfaces.Contains(todoRename.AsType()));
            Assert.Contains(compiledTypes, t => t.Name == nameof(PolymorphicClassAB) && t.ImplementedInterfaces.Contains(todoRename.AsType()));
            var todoRename1 = Assert.Single(compiledTypes, t => t.Name == "ITodoRename1" && t.IsInterface);
            Assert.Contains(compiledTypes, t => t.Name == "TodoRename1Unknown" && t.IsClass && t.ImplementedInterfaces.Contains(todoRename1.AsType()));
            Assert.Contains(compiledTypes, t => t.Name == nameof(PolymorphicClassBA) && t.ImplementedInterfaces.Contains(todoRename1.AsType()));
            Assert.Contains(compiledTypes, t => t.Name == nameof(PolymorphicClassBB) && t.ImplementedInterfaces.Contains(todoRename1.AsType()));
            Assert.Contains(compiledTypes, t => t.Name == nameof(Polymorphic));
            Assert.DoesNotContain(compiledTypes, t => t.Name == nameof(PolymorphicClassA));
            Assert.DoesNotContain(compiledTypes, t => t.Name == nameof(PolymorphicClassB));

            /*List<Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>> serializers = BinaryDeserializerBuilder.CreateDefaultCaseBuilders().ToList();

            // Prepend our custom case to handle the polymorphic Animal type
            serializers.Insert(0, builder => new PolymorphicClassAUnionDeserializerBuilderCase(builder));
            serializers.Insert(0, builder => new PolymorphicClassBUnionDeserializerBuilderCase(builder));

            // Optionally, check compatibility
            CompatibilityChecker.AssertCanDeserializeTypeFromSchema(new BinaryDeserializerBuilder(serializers), generatedType, schema);
            */
        }

        // Class with polymorphic field
        private class Polymorphic
        {
            public PolymorphicClassA AbstractFieldA { get; set; }

            public PolymorphicClassAA DerivedFieldAA { get; set; }

            public PolymorphicClassA[] AbstractCollectionA { get; set; }

            public Dictionary<string, PolymorphicClassA> AbstractMapA { get; set; }

            public PolymorphicClassB AbstractFieldB { get; set; }

            public PolymorphicClassBA DerivedFieldBA { get; set; }

            public PolymorphicClassB[] AbstractCollectionB { get; set; }

            public Dictionary<string, PolymorphicClassB> AbstractMapB { get; set; }
        }
    }
}
