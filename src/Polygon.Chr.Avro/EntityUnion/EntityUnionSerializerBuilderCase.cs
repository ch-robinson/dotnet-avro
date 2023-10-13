using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace Polygon.Chr.Avro.EntityUnion
{
    public class EntityUnionSerializerBuilderCase : JsonUnionSerializerBuilderCase
    {
        private UnionRegistry UnionRegistry { get; }
        public EntityUnionSerializerBuilderCase(IJsonSerializerBuilder serializerBuilder, UnionRegistry registry)
            : base(serializerBuilder)
        {
            UnionRegistry = registry;
        }

        public override JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {

            if (UnionRegistry.Types.ContainsKey(type))
            {
                var bb = base.BuildExpression(value, type, schema, context);
                return bb;
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(EntityUnionSerializerBuilderCase)} cannot be applied here."));
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
