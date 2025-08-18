namespace Chr.Avro.Fixtures
{
    using System;
    using Chr.Avro.Abstract;

    public class PolymorphicClassAUnionSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        private readonly ISchemaBuilder schemaBuilder;

        public PolymorphicClassAUnionSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            this.schemaBuilder = schemaBuilder;
        }

        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(PolymorphicClassA))
            {
                return SchemaBuilderCaseResult.FromSchema(
                    new UnionSchema(new[]
                    {
                        schemaBuilder.BuildSchema<PolymorphicClassAA>(context),
                        schemaBuilder.BuildSchema<PolymorphicClassAB>(context),
                    }));
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(PolymorphicClassAUnionSchemaBuilderCase)} can only be applied to the {typeof(PolymorphicClassA)} type."));
            }
        }
    }
}
