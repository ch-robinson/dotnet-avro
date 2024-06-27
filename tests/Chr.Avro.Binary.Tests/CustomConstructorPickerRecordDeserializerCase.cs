namespace Chr.Avro.Serialization.Tests;

using System;
using System.Linq;
using System.Reflection;
using Chr.Avro.Abstract;

internal class CustomConstructorPickerRecordDeserializerCase : BinaryRecordDeserializerBuilderCase
{
    public CustomConstructorPickerRecordDeserializerCase(IBinaryDeserializerBuilder deserializerBuilder)
        : base(deserializerBuilder, BindingFlags.Public | BindingFlags.Instance)
    {
    }

    protected override ConstructorInfo GetRecordConstructor(Type type, RecordSchema schema)
    {
        // Return the constructor marked with [ChrPreferredConstructorAttribute]
        var constructor = type.GetConstructors()
            .Where(c => c.GetCustomAttribute<ChrPreferredConstructorAttribute>() != null)
            .FirstOrDefault();

        if (constructor != null)
        {
            return constructor;
        }

        return base.GetRecordConstructor(type, schema);
    }
}
