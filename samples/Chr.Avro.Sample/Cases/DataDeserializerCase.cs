using System;
using System.Collections.Generic;
using System.Text;
using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using Chr.Avro.Sample.Models;
using Chr.Avro.Serialization;

namespace Chr.Avro.Sample.Cases
{
    class DataDeserializerCase : UnionDeserializerBuilderCase
    {
        public ITypeResolver Resolver { get; }

        public DataDeserializerCase(ITypeResolver resolver, IBinaryCodec codec, IBinaryDeserializerBuilder builder) : base(codec, builder)
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
