namespace Chr.Avro.Codegen.Tests.Codegen
{
    using System.ComponentModel;
    using System.Linq;
    using System.Reflection;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Xunit;

    public class GenerateDocumentationAttributesTests
    {
        [Fact]
        public void GivenClassesFieldsAndPropertiesWithDocumentation_WhenEnablingDescriptionAttributes_ShouldBeGeneratedWithDescriptionAttributes()
        {
            var generatedClass = GetTypeGeneratedViaSchema<DescriptionAnnotatedClass>(enableDescriptionAttribute: true);

            var classAttribute = generatedClass.GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Class Description", classAttribute?.Description);

            var fieldAttribute = generatedClass.GetProperty(nameof(DescriptionAnnotatedClass.DescriptionField)).GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Field Description", fieldAttribute?.Description);

            var propertyAttribute = generatedClass.GetProperty(nameof(DescriptionAnnotatedClass.DescriptionProperty)).GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Property Description", propertyAttribute?.Description);

            var propertyAttributeWithDoubleQuotes = generatedClass.GetProperty(nameof(DescriptionAnnotatedClass.DescriptionPropertyWithDoubleQuotes)).GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Property \"Description\" with double quotes", propertyAttributeWithDoubleQuotes?.Description);
        }

        [Fact]
        public void GivenEnumWithDocumentation_WhenEnablingDescriptionAttributes_ShouldBeGeneratedWithDescriptionAttribute()
        {
            var generatedEnum = GetTypeGeneratedViaSchema<DescriptionAnnotatedEnum>(enableDescriptionAttribute: true);

            var enumAttribute = generatedEnum.GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Enum Description", enumAttribute?.Description);
        }

        [Fact]
        public void GivenClassesFieldsAndPropertiesWithDocumentation_WhenDisablingDescriptionAttributes_ShouldNotBeGeneratedWithDescriptionAttributes()
        {
            var generatedClass = GetTypeGeneratedViaSchema<DescriptionAnnotatedClass>(enableDescriptionAttribute: false);

            var classAttribute = generatedClass.GetCustomAttribute<DescriptionAttribute>();
            Assert.Null(classAttribute);

            var fieldAttribute = generatedClass.GetProperty(nameof(DescriptionAnnotatedClass.DescriptionField)).GetCustomAttribute<DescriptionAttribute>();
            Assert.Null(fieldAttribute);

            var propertyAttribute = generatedClass.GetProperty(nameof(DescriptionAnnotatedClass.DescriptionProperty)).GetCustomAttribute<DescriptionAttribute>();
            Assert.Null(propertyAttribute);
        }

        [Fact]
        public void GivenEnumWithDocumentation_WhenDisablingDescriptionAttributes_ShouldNotBeGeneratedWithDescriptionAttribute()
        {
            var generatedEnum = GetTypeGeneratedViaSchema<DescriptionAnnotatedEnum>(enableDescriptionAttribute: false);

            var enumAttribute = generatedEnum.GetCustomAttribute<DescriptionAttribute>();
            Assert.Null(enumAttribute);
        }

        private static TypeInfo GetTypeGeneratedViaSchema<T>(bool enableDescriptionAttribute)
        {
            var schema = new SchemaBuilder().BuildSchema<T>();
            var codeGenerator = new CSharpCodeGenerator(enableDescriptionAttributeForDocumentation: enableDescriptionAttribute);
            var sourceCode = codeGenerator.WriteCompilationUnit(schema);
            var compiledTypes = CSharpCodeCompiler.GetTypesDefinedInSourceCode(sourceCode);

            var generatedType = compiledTypes.Single(x => x.Name == typeof(T).Name);

            CompatibilityChecker.AssertCanDeserializeTypeFromSchema(generatedType, schema);

            return generatedType;
        }
    }
}
