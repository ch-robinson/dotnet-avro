using Chr.Avro;
using Chr.Avro.Abstract;
using System;
using System.Linq;

namespace Polygon.Chr.Avro.EntityUnion
{
    public class EntityUnionSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        private readonly ISchemaBuilder schemaBuilder;
        private UnionRegistry UnionRegistry { get; }

        public EntityUnionSchemaBuilderCase(ISchemaBuilder schemaBuilder, UnionRegistry unionRegistry)
        {
            this.schemaBuilder = schemaBuilder;
            UnionRegistry = unionRegistry;
        }

        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (UnionRegistry.Types.ContainsKey(type))
            {
                var schemae = UnionRegistry.Types[type]
                    .Select(z => schemaBuilder.BuildSchema(z, context))
                    .Select(z => ((UnionSchema)z).Schemas.ElementAt(1))
                    .ToArray();
                return SchemaBuilderCaseResult.FromSchema(
                    new UnionSchema(schemae));
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(EntityUnionSchemaBuilderCase)} cannot be applied here."));
            }
        }
    }
}
