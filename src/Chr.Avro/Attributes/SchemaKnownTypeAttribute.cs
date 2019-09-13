using System;

namespace Chr.Avro.Attributes
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = true)]
    public class SchemaKnownTypeAttribute: Attribute
    {
        public Type Type { get; set; }
        public SchemaKnownTypeAttribute(Type type)
        {
            Type = type;
        }

        
    }
}
