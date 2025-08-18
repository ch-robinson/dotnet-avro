namespace Chr.Avro.Fixtures
{
    using System;
    using Chr.Avro.Abstract;

    public class PolymorphicClassBUnionSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        private readonly ISchemaBuilder schemaBuilder;

        public PolymorphicClassBUnionSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            this.schemaBuilder = schemaBuilder;
        }

        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(PolymorphicClassB))
            {
                return SchemaBuilderCaseResult.FromSchema(
                    new UnionSchema(new[]
                    {
                        schemaBuilder.BuildSchema<PolymorphicClassBA>(context),
                        schemaBuilder.BuildSchema<PolymorphicClassBB>(context),
                    }));
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(PolymorphicClassBUnionSchemaBuilderCase)} can only be applied to the {typeof(PolymorphicClassB)} type."));
            }
        }
    }
}
