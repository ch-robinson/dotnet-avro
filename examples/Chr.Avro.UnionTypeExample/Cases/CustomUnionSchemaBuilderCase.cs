namespace Chr.Avro.UnionTypeExample.Cases
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.UnionTypeExample.Models;

    public class CustomUnionSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        private readonly ISchemaBuilder schemaBuilder;

        public CustomUnionSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            this.schemaBuilder = schemaBuilder;
        }

        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(IDataObj))
            {
                return SchemaBuilderCaseResult.FromSchema(
                    new UnionSchema(new[]
                    {
                        schemaBuilder.BuildSchema<DataObj1>(context),
                        schemaBuilder.BuildSchema<DataObj2>(context),
                        schemaBuilder.BuildSchema<DataObj3>(context),
                    }));
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(CustomUnionSchemaBuilderCase)} can only be applied to the {typeof(IDataObj)} type."));
            }
        }
    }
}
