using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using System;
using System.Linq;

namespace Polygon.Chr.Avro.EntityUnion
{
    public class EntityUnionDeserializerBuilderCase : JsonUnionDeserializerBuilderCase
    {
        private UnionRegistry UnionRegistry { get; }
        public EntityUnionDeserializerBuilderCase(IJsonDeserializerBuilder deserializerBuilder, UnionRegistry registry)
            : base(deserializerBuilder)
        {
            UnionRegistry = registry;
        }

        public override JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {

            if (UnionRegistry.Types.ContainsKey(type))
            {
                return base.BuildExpression(type, schema, context);
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(EntityUnionDeserializerBuilderCase)} cannot be applied here."));
            }
        }

        protected override Type SelectType(Type type, Schema schema)
        {
            if (!UnionRegistry.Types.ContainsKey(type))
                throw new UnsupportedSchemaException(schema);
            var rs = schema as RecordSchema;
            if (rs == null)
                throw new UnsupportedSchemaException(schema);


            return UnionRegistry.Types[type].Single(z => z.Name == rs.Name);

        }
    }
}
