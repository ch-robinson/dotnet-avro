namespace Chr.Avro.Codegen.Tests
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
        public void ClassesFieldsAndPropertiesWithDescriptionAttribute_ShouldBeGeneratedWithDescriptionAttributes()
        {
            var schema = new SchemaBuilder().BuildSchema(typeof(DescriptionAnnotatedClass));
            var generatedTypes = TestHelper.GetTypesGeneratedFromSchema<DescriptionAnnotatedClass>(schema);

            var generatedClass = generatedTypes.Single(x => x.Name == nameof(DescriptionAnnotatedClass));

            var classAttribute = generatedClass.GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Class Description", classAttribute?.Description);

            var fieldAttribute = generatedClass.GetProperty("DescriptionField").GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Field Description", fieldAttribute?.Description);

            var propertyAttribute = generatedClass.GetProperty("DescriptionProperty").GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Property Description", propertyAttribute?.Description);

            TestHelper.AssertCanDeserializeTypeFromSchema(generatedClass, schema);
        }

        [Fact]
        public void EnumWithDescriptionAttribute_ShouldBeGeneratedWithDescriptionAttribute()
        {
            var schema = new SchemaBuilder().BuildSchema(typeof(DescriptionAnnotatedEnum));
            var generatedTypes = TestHelper.GetTypesGeneratedFromSchema<DescriptionAnnotatedEnum>(schema);

            var generatedEnum = generatedTypes.Single(x => x.Name == nameof(DescriptionAnnotatedEnum));

            var enumAttribute = generatedEnum.GetCustomAttribute<DescriptionAttribute>();
            Assert.Equal("Enum Description", enumAttribute?.Description);

            TestHelper.AssertCanDeserializeTypeFromSchema(generatedEnum, schema);
        }
    }
}
