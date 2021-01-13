using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using Chr.Avro.Sample.Models;

namespace Chr.Avro.Sample.Cases
{
    public class MyBuilderCases : SchemaBuilderCase
    {
        private readonly ISchemaBuilder _schemaBuilder;

        public MyBuilderCases(ISchemaBuilder schemaBuilder)
        {
            _schemaBuilder = schemaBuilder;
        }

        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();
            if (resolution is RecordResolution && resolution.Type == typeof(IDataObj))
            {
                Schema schema = new UnionSchema(new List<Schema>()
                {
                    _schemaBuilder.BuildSchema<DataObj1>(cache),
                    _schemaBuilder.BuildSchema<DataObj2>(cache),
                    _schemaBuilder.BuildSchema<DataObj3>(cache)
                });
                result.Schema = schema;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
        }
    }
}