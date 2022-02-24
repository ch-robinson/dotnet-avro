using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using Chr.Avro.UnionTypeExample.Models;
using Chr.Avro.Serialization;

namespace Chr.Avro.UnionTypeExample.Cases
{
    public class DataSerializerCase : UnionSerializerBuilderCase
    {
        public ITypeResolver Resolver { get; }

        public DataSerializerCase(ITypeResolver resolver, IBinaryCodec codec, IBinarySerializerBuilder builder) : base(codec, builder)
        {
            Resolver = resolver;

        }

        protected override TypeResolution SelectType(TypeResolution resolution, Schema schema)
        {
            if (!(resolution is RecordResolution recordResolution) || recordResolution.Type != typeof(IDataObj))
            {
                throw new UnsupportedTypeException(resolution.Type);
            }

            switch ((schema as RecordSchema)?.Name)
            {
                case nameof(DataObj1):
                    return Resolver.ResolveType<DataObj1>();

                case nameof(DataObj2):
                    return Resolver.ResolveType<DataObj2>();

                case nameof(DataObj3):
                    return Resolver.ResolveType<DataObj3>();

                default:
                    throw new UnsupportedSchemaException(schema);
            }
        }

    }
}
